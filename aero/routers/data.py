import logging
import re

from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse
from uuid import UUID

import requests

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Header
from fastapi import HTTPException
from fastapi import Query

from pydantic import BaseModel
from pydantic import Field

from sqlalchemy import or_

from sqlmodel import select
from sqlmodel import Session

from aero.auth import require_globus_auth
from aero.config import Config
from aero.database import get_session
from aero.models.data import create_data
from aero.models.data import Data
from aero.models.data_file import DataFile
from aero.models.data_version import DataVersion
from aero.models.flows import Flow
from aero.models.flows import TriggerEnum
from aero.models.provenance import Provenance
from aero.models.source_type import SourceType
from aero.models.source_type import SourceUrl
from aero.models.types import utcnow

from aero import GLOBUS_CLIENT

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/data",
    tags=["data"],
    dependencies=[Depends(require_globus_auth)],
    responses={404: {"description": "Not found"}},
)

# Webhook routes use the shared-secret (X-Aero-Token) scheme instead of a Globus
# user token — the caller is an automated S3 event pipeline, not a Globus user —
# so they live on a separate router without the Globus auth dependency.
webhook_router = APIRouter(
    prefix="/data",
    tags=["webhook"],
    responses={404: {"description": "Not found"}},
)


class VersionOut(BaseModel):
    id: UUID = Field()
    version: int | None = Field(default=None)
    checksum: str | None = Field(default=None)
    created_at: datetime | None = Field(default=None)
    data_id: UUID | None = Field(default=None)
    data: Optional["Data"] = Field(default=None)
    data_file: Optional["DataFile"] = Field(default=None)
    # No-copy sources store no bytes, so the analysis pull needs the object's own
    # url. source_key identifies which registered url produced this version, and
    # trigger_url is that url resolved — so a run that wasn't triggered by this
    # source can still locate it.
    source_key: str | None = Field(default=None)
    no_copy: bool = Field(default=False)
    trigger_url: str | None = Field(default=None)


@router.get("/", response_model=list[Data])
# @authenticated
def all_data(
    offset: int = 0,
    limit: int = Query(default=15, le=15),
    session: Session = Depends(get_session),
):
    data = session.exec(
        select(Data).order_by(Data.id.desc()).offset(offset).limit(limit)
    ).all()
    return data


@router.get("/search")
# @authenticated
def search(query: str) -> dict:
    sc = GLOBUS_CLIENT.search_client

    try:
        result = sc.client.search(sc.index, query, advanced=True)
    except Exception as e:
        return HTTPException(status_code=500, detail=str(e))
    return result.data


class SourceUrlOut(BaseModel):
    url: str = Field()
    object_key: str = Field()


class SourceTypeOut(BaseModel):
    name: str = Field()
    data_id: UUID = Field()
    no_copy: bool = Field(default=False)
    urls: list[SourceUrlOut] = Field(default_factory=list)


class SourceUrlIn(BaseModel):
    url: str = Field()


class SourceIn(BaseModel):
    """Create a source ``Data`` directly, with no flow attached."""

    name: str = Field()
    url: str = Field()
    type: str | None = Field(default=None)
    no_copy: bool = Field(default=False)
    collection_uuid: UUID | None = Field(default=None)
    collection_url: str | None = Field(default=None)
    description: str | None = Field(default=None)


def _type_out(st: SourceType) -> SourceTypeOut:
    return SourceTypeOut(
        name=st.name,
        data_id=st.data_id,
        no_copy=bool(st.data.no_copy) if st.data is not None else False,
        urls=[SourceUrlOut(url=u.url, object_key=u.object_key) for u in st.urls],
    )


def _register_source_url(session: Session, st: SourceType, url: str) -> SourceUrl:
    """Attach a URL to a type, rejecting a key already claimed elsewhere.

    The key is unique across all types: it is what a notify resolves on, so two
    types owning the same object would make resolution ambiguous by construction.
    """
    object_key = _normalize_registered_key(url)
    clash = session.exec(
        select(SourceUrl).where(SourceUrl.object_key == object_key)
    ).first()
    if clash is not None:
        if clash.type_id == st.id:
            return clash
        raise HTTPException(
            status_code=409,
            detail=(
                f"Object key '{object_key}' is already registered to type "
                f"'{clash.type.name}'."
            ),
        )

    su = SourceUrl(type_id=st.id, url=url, object_key=object_key)
    session.add(su)
    session.commit()
    session.refresh(su)
    return su


def create_source_type(
    session: Session, name: str, data: Data, url: str | None = None
) -> SourceType:
    """Bind a new type to a Data, optionally registering its first URL."""
    existing = session.exec(select(SourceType).where(SourceType.name == name)).first()
    if existing is not None:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Source type '{name}' already exists (data {existing.data_id}). "
                "Add a url to it instead of creating it again."
            ),
        )

    st = SourceType(name=name, data_id=data.id)
    session.add(st)
    session.commit()
    session.refresh(st)

    if url:
        _register_source_url(session, st, url)
        session.refresh(st)

    return st


@router.get("/types", response_model=list[SourceTypeOut])
def list_source_types(session: Session = Depends(get_session)):
    """Every notification type with its Data UUID and registered urls."""
    return [_type_out(st) for st in session.exec(select(SourceType)).all()]


@router.get("/types/{name}", response_model=SourceTypeOut)
def get_source_type(name: str, session: Session = Depends(get_session)):
    st = session.exec(select(SourceType).where(SourceType.name == name)).first()
    if st is None:
        raise HTTPException(status_code=404, detail=f"Source type '{name}' not found.")
    return _type_out(st)


@router.post("/types/{name}/urls", response_model=SourceTypeOut)
def add_source_type_url(
    name: str, payload: SourceUrlIn, session: Session = Depends(get_session)
):
    """Associate another URL with an existing type.

    The type's Data UUID is unchanged, so analysis flows already registered against
    it now also fire when this new URL changes.
    """
    st = session.exec(select(SourceType).where(SourceType.name == name)).first()
    if st is None:
        raise HTTPException(status_code=404, detail=f"Source type '{name}' not found.")

    _register_source_url(session, st, payload.url)
    session.refresh(st)
    return _type_out(st)


@router.delete("/types/{name}/urls", response_model=SourceTypeOut)
def delete_source_type_url(
    name: str, url: str, session: Session = Depends(get_session)
):
    """Remove one URL from a type, leaving the type and its Data in place.

    The url is normalized the way registration normalizes it, then — if that finds
    nothing — the way a notify identity is normalized, so a presigned url pasted
    out of a log resolves too. The two differ on ``?``: a registered key may be a
    glob, where a notify identity's ``?`` always starts a query string.

    Only the matching rule goes: the type, its Data UUID, its version history and
    every flow registered against it are untouched, and the key is free for another
    type to claim.

    A type left with no urls is unreachable by notify — resolution falls back to
    untyped sources only — so for a no-copy source that is the end of ingestion
    until a url is added back.
    """
    st = session.exec(select(SourceType).where(SourceType.name == name)).first()
    if st is None:
        raise HTTPException(status_code=404, detail=f"Source type '{name}' not found.")

    object_key = _normalize_registered_key(url)
    su = session.exec(
        select(SourceUrl).where(SourceUrl.object_key == object_key)
    ).first()

    if su is None:
        concrete = _normalize_object_key(url)
        if concrete != object_key:
            su = session.exec(
                select(SourceUrl).where(SourceUrl.object_key == concrete)
            ).first()
            if su is not None:
                object_key = concrete

    if su is None:
        raise HTTPException(
            status_code=404,
            detail=f"Object key '{object_key}' is not registered to any type.",
        )
    if su.type_id != st.id:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Object key '{object_key}' belongs to type '{su.type.name}', "
                f"not '{name}'."
            ),
        )

    session.delete(su)
    session.commit()
    session.refresh(st)
    return _type_out(st)


@router.post("/source", response_model=Data)
def create_source(payload: SourceIn, session: Session = Depends(get_session)):
    """Create a source ``Data`` with no flow attached.

    This is the no-copy create path: nothing is pulled, so there is no endpoint,
    no Globus function and no ingestion flow — the notify webhook records versions
    directly from the event metadata.
    """
    if payload.type is None and _is_pattern(payload.url):
        raise HTTPException(
            status_code=400,
            detail=(
                f"'{payload.url}' is a glob pattern. Patterns are matching rules "
                "for a type's objects, so they need a 'type'; an untyped source "
                "resolves by its own url and nothing would ever match this."
            ),
        )

    d = create_data(
        session=session,
        name=payload.name,
        url=payload.url,
        collection_uuid=payload.collection_uuid,
        collection_url=payload.collection_url,
        description=payload.description,
    )
    d.no_copy = payload.no_copy
    session.add(d)
    session.commit()
    session.refresh(d)

    if payload.type:
        create_source_type(session=session, name=payload.type, data=d, url=payload.url)
        session.refresh(d)

    return d


@router.get("/{id}", response_model=Data)
# @authenticated
def get_data(id: UUID, session: Session = Depends(get_session)):
    d = session.exec(select(Data).where(Data.id == id)).first()
    if d is None:
        raise HTTPException(status_code=404, detail=f"Data with id {id} not found.")
    return d


@router.get("/{id}/versions", response_model=list[DataVersion])
# @authenticated
def list_versions(id: UUID, session: Session = Depends(get_session)):
    d = session.exec(select(Data).where(Data.id == id)).first()
    if d is None:
        raise HTTPException(status_code=404, detail=f"Data with id {id} not found.")

    return d.versions


@router.get("/{id}/latest", response_model=VersionOut)
# @authenticated
def get_latest(id: UUID, session: Session = Depends(get_session)):
    d = session.exec(select(Data).where(Data.id == id)).first()
    if d is None:
        raise HTTPException(status_code=404, detail=f"Data with id {id} not found.")

    version = d.last_version()

    if version is None:
        raise HTTPException(
            status_code=404, detail="No versions exist for this data ID."
        )

    return VersionOut(
        id=version.id,
        version=version.version,
        checksum=version.checksum,
        created_at=version.created_at,
        data_id=version.data_id,
        data=d,
        data_file=version.data_file,
        source_key=version.source_key,
        no_copy=bool(d.no_copy),
        trigger_url=_resolve_trigger_url(session, d, version.source_key),
    )


def _attached_flows(session: Session, data: Data) -> list[tuple[Flow, str]]:
    """Every flow directly touching this Data, each with the role it plays.

    "ingestion" produces the Data, "analysis" consumes it, "both" does each --
    possible when a flow's output feeds back into its own input.
    """
    flows = session.exec(
        select(Flow).where(
            or_(
                Flow.derived_from.any(id=data.id),
                Flow.contributed_to.any(id=data.id),
            )
        )
    ).all()

    tagged = []
    for f in flows:
        produces = any(d.id == data.id for d in f.contributed_to)
        consumes = any(d.id == data.id for d in f.derived_from)
        role = "both" if produces and consumes else ("ingestion" if produces else "analysis")
        tagged.append((f, role))
    return tagged


class AttachedFlowOut(BaseModel):
    flow_id: UUID = Field()
    role: str = Field()  # ingestion | analysis | both
    policy: int | None = Field(default=None)
    description: str | None = Field(default=None)
    timer_job_id: UUID | None = Field(default=None)
    last_executed: datetime | None = Field(default=None)


class DeletedFlowOut(AttachedFlowOut):
    provenance_deleted: int = Field(default=0)
    timer: str | None = Field(default=None)  # cancelled | the failure, if any


@router.get("/{id}/flows", response_model=list[AttachedFlowOut])
def list_data_flows(id: UUID, session: Session = Depends(get_session)):
    """The flows attached to this Data — what produces it and what consumes it."""
    d = session.exec(select(Data).where(Data.id == id)).first()
    if d is None:
        raise HTTPException(status_code=404, detail=f"Data with id {id} not found.")

    return [
        AttachedFlowOut(
            flow_id=f.id,
            role=role,
            policy=f.policy,
            description=f.description,
            timer_job_id=f.timer_job_id,
            last_executed=f.last_executed,
        )
        for f, role in _attached_flows(session, d)
    ]


@router.delete("/{id}/flows", response_model=list[DeletedFlowOut])
def delete_data_flows(id: UUID, session: Session = Depends(get_session)):
    """Delete every flow attached to this Data, and their provenance records.

    The Data itself, its versions and any source type/urls are left alone, so
    flows can be re-registered against the same UUID.

    Provenance goes too: ``Provenance.flow_id`` is a non-nullable foreign key, so
    a flow that has ever run cannot be removed while its records remain. This
    discards which input versions those runs consumed.
    """
    d = session.exec(select(Data).where(Data.id == id)).first()
    if d is None:
        raise HTTPException(status_code=404, detail=f"Data with id {id} not found.")

    deleted = []
    for flow, role in _attached_flows(session, d):
        entry = DeletedFlowOut(
            flow_id=flow.id,
            role=role,
            policy=flow.policy,
            description=flow.description,
            timer_job_id=flow.timer_job_id,
            last_executed=flow.last_executed,
        )

        # Cancel the Globus timer first, but never let its failure block the
        # delete: a timer whose flow row is gone keeps firing, and its worker
        # posts to /prov/new against a flow_id that no longer exists. Reporting
        # the job id is what lets an uncancellable one be cleaned up by hand.
        if flow.timer_job_id is not None:
            try:
                GLOBUS_CLIENT.delete_job(str(flow.timer_job_id))
                entry.timer = "cancelled"
            except Exception as e:
                entry.timer = f"NOT cancelled: {type(e).__name__}: {e}"
                logger.warning(
                    "could not cancel timer job %s for flow %s; it will keep "
                    "firing against a deleted flow: %s",
                    flow.timer_job_id,
                    flow.id,
                    e,
                )

        provenances = session.exec(
            select(Provenance).where(Provenance.flow_id == flow.id)
        ).all()
        entry.provenance_deleted = len(provenances)
        for p in provenances:
            session.delete(p)
        session.commit()

        # The flowderivation/flowcontribution rows go with it: SQLAlchemy clears
        # secondary-table rows when the parent is deleted.
        session.delete(flow)
        session.commit()

        logger.info(
            "deleted %s flow %s for data %s (%d provenance records)",
            role,
            flow.id,
            d.id,
            entry.provenance_deleted,
        )
        deleted.append(entry)

    return deleted


class NotifyIn(BaseModel):
    """Optional S3 event fields, accepted for traceability. Not required for the
    pull itself — the ingestion flow's download function re-fetches the source
    url, and the version metadata comes from the pulled file."""

    url: str | None = Field(default=None)  # per-run source url (e.g. MinIO presigned)
    key: str | None = Field(default=None)
    etag: str | None = Field(default=None)
    size: int | None = Field(default=None)
    # False treats this notify as a change even when the etag is unchanged.
    dedup: bool = Field(default=True)


class NotifyByObjectIn(BaseModel):
    """Notify keyed by object identity instead of Data UUID.

    ``file_id`` is the object's *stable* identity — a full object URL or a bare
    ``bucket/key`` — used to resolve the Data record. ``url`` is the transient
    per-run source (e.g. a MinIO presigned GET) used only for this run's pull."""

    file_id: str = Field()  # object identity: full URL or bare "bucket/key"
    url: str | None = Field(default=None)  # per-run source url (e.g. presigned)
    key: str | None = Field(default=None)
    etag: str | None = Field(default=None)
    size: int | None = Field(default=None)
    # False treats this notify as a change even when the etag is unchanged.
    dedup: bool = Field(default=True)


def _normalize_object_key(value: str) -> str:
    """Reduce an object URL (or bare ``bucket/key``) to a stable ``bucket/key``.

    Drops scheme, host, port, and query string so that a presigned URL, a plain
    object URL, and a bare ``bucket/key`` for the same object all compare equal:

        http://localhost:9000/traffic/report.xml.gz?X-Amz-Signature=... ->
        http://127.0.0.1:9000/traffic/report.xml.gz                     ->
        traffic/report.xml.gz                                           ->
            "traffic/report.xml.gz"
    """
    parsed = urlparse(value)
    # A scheme (http/https/s3/...) means host+path form; else treat as bucket/key.
    path = parsed.path if parsed.scheme else value
    return path.strip("/")


def _normalize_registered_key(value: str) -> str:
    """Normalize a url being *registered*, which may be a glob pattern.

    Patterns can't go through ``_normalize_object_key``: ``urlparse`` treats ``?``
    as the start of a query string, so ``bucket/report-?.csv`` would be truncated
    to ``bucket/report-``. Here ``?`` is a glob, so strip the scheme and host by
    hand and keep the rest.

    Notify identities are always concrete objects and keep using
    ``_normalize_object_key``, which must go on discarding presigned query strings.
    """
    if not _is_pattern(value):
        return _normalize_object_key(value)

    if "://" in value:
        after_scheme = value.split("://", 1)[1]
        # everything past the host, or nothing if the url is just a host
        value = after_scheme.split("/", 1)[1] if "/" in after_scheme else ""
    return value.strip("/")


def _normalize_full_url(value: str) -> str:
    """Reduce a URL to ``host:port/path`` for disambiguating same-key sources.

    Keeps the netloc (host+port) that ``_normalize_object_key`` discards, so two
    sources sharing a ``bucket/key`` on different hosts can be told apart. Scheme
    and query are still dropped, so an ``http`` vs ``https`` or presigned-vs-plain
    URL for the same object still compares equal. A bare ``bucket/key`` (no host)
    normalizes to just the key, which won't match a host-qualified registered url.
    """
    parsed = urlparse(value)
    if parsed.scheme:
        return f"{parsed.netloc}{parsed.path}".strip("/")
    return value.strip("/")


_GLOB_CHARS = "*?["


def _is_pattern(key: str) -> bool:
    """Whether a registered key is a glob rather than one specific object."""
    return any(c in key for c in _GLOB_CHARS)


@lru_cache(maxsize=256)
def _pattern_regex(pattern: str) -> re.Pattern:
    """Compile a glob pattern to an anchored regex, with canonical semantics.

    ``*`` matches within one path segment, ``**`` spans segments, ``?`` is a
    single character, and ``[...]`` is a character class. Python 3.11 has no
    stdlib equivalent — ``glob.translate`` and ``PurePath.full_match`` are 3.13+,
    and ``fnmatch``'s ``*`` crosses ``/``.

        **/test-data/**       every object under a test-data/ dir at any depth
        test-bucket/**/*.csv  every .csv at any depth in test-bucket
        test-bucket/*/*.csv   .csv files exactly one level deep
    """
    out: list[str] = []
    i, n = 0, len(pattern)

    while i < n:
        c = pattern[i]
        if c == "*":
            if pattern[i : i + 2] == "**":
                i += 2
                if pattern[i : i + 1] == "/":
                    # "**/" spans zero or more whole segments, so that
                    # a/**/b.csv matches both a/b.csv and a/x/y/b.csv.
                    out.append("(?:[^/]+/)*")
                    i += 1
                else:
                    out.append(".*")
            else:
                out.append("[^/]*")
                i += 1
        elif c == "?":
            out.append("[^/]")
            i += 1
        elif c == "[":
            close = pattern.find("]", i)
            if close == -1:  # unterminated: a literal bracket
                out.append(re.escape(c))
                i += 1
            else:
                out.append("[" + pattern[i + 1 : close].replace("\\", "\\\\") + "]")
                i = close + 1
        else:
            out.append(re.escape(c))
            i += 1

    return re.compile(r"\A" + "".join(out) + r"\Z")


def _pattern_specificity(pattern: str) -> tuple[int, int, str]:
    """Sort key ranking patterns by how narrowly they match.

    The literal prefix before the first glob character dominates, so
    ``test-bucket/logs/**`` outranks ``**/*.csv`` for an object under
    ``test-bucket/logs/``. Length and then the string itself break ties, so the
    winner never depends on row order.
    """
    first_glob = re.search(r"[*?\[]", pattern)
    literal = len(pattern) if first_glob is None else first_glob.start()
    return (literal, len(pattern), pattern)


def _concrete_object_url(registered_url: str, object_key: str) -> str:
    """The fetchable URL of one object, given the entry that matched it.

    A pattern's registered url (``http://minio:9000/bucket/**/*.csv``) is not
    fetchable, so rebuild the real one from its scheme and host plus the concrete
    key. An entry with no scheme has no host to contribute; return the key.
    """
    parsed = urlparse(registered_url)
    if parsed.scheme:
        return f"{parsed.scheme}://{parsed.netloc}/{object_key}"
    return object_key


def _match_source_url(
    urls: list[SourceUrl], object_key: str
) -> SourceUrl | None:
    """Pick the entry that claims ``object_key``: exact first, else most specific.

    Raises 409 only when two patterns are equally specific, which cannot be
    resolved without guessing.
    """
    for su in urls:
        if su.object_key == object_key:
            return su

    matches = [
        su
        for su in urls
        if _is_pattern(su.object_key)
        and _pattern_regex(su.object_key).match(object_key)
    ]
    if not matches:
        return None

    matches.sort(key=lambda su: _pattern_specificity(su.object_key), reverse=True)
    if len(matches) > 1:
        best, runner_up = matches[0], matches[1]
        if _pattern_specificity(best.object_key)[:2] == _pattern_specificity(
            runner_up.object_key
        )[:2]:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"'{object_key}' is matched equally well by patterns "
                    f"'{best.object_key}' and '{runner_up.object_key}'."
                ),
            )
    return matches[0]


def _require_webhook_token(x_aero_token: str | None) -> None:
    """Guard webhook routes with the shared secret when one is configured."""
    if Config.WEBHOOK_SECRET is not None and x_aero_token != Config.WEBHOOK_SECRET:
        raise HTTPException(
            status_code=401, detail="Invalid or missing webhook token."
        )


def _resolve_trigger_url(
    session: Session, data: Data, object_key: str | None
) -> str | None:
    """The fetchable url of one object, or None if it cannot be determined.

    Lets any run locate a no-copy source's bytes, not only the run its notify
    triggered — the signature is what can't be reconstructed, the url always can.
    A pattern is never returned: it is a matching rule, and handing it back would
    send the analysis worker off to download a literal ``**``.
    """
    if object_key:
        su = session.exec(
            select(SourceUrl).where(SourceUrl.object_key == object_key)
        ).first()
        if su is not None:
            return su.url

        if data.source_type is not None:
            match = _match_source_url(list(data.source_type.urls), object_key)
            if match is not None:
                return _concrete_object_url(match.url, object_key)

    if data.url and _is_pattern(data.url):
        return None
    return data.url


def _resolve_notify_target(session: Session, file_id: str) -> tuple[Data, str, str]:
    """Resolve an object identity to ``(Data, object_key, trigger_url)``.

    Registered ``SourceUrl`` entries win. An exact key is an indexed lookup — the
    key is normalized once at registration rather than once per row per notify —
    and only if that misses are the (few) pattern entries scanned. Untyped sources
    fall back to the original ``Data.url`` scan, which skips typed Data so the two
    can't double-match.

    The object key returned is always the **concrete** one, never the pattern that
    matched it: it becomes ``DataVersion.source_key``, so a pattern standing in for
    it would collapse every object it matches into a single dedup bucket.
    """
    target = _normalize_object_key(file_id)

    su = session.exec(
        select(SourceUrl).where(SourceUrl.object_key == target)
    ).first()
    if su is not None:
        return su.type.data, su.object_key, su.url

    patterns = session.exec(
        select(SourceUrl).where(
            or_(*(SourceUrl.object_key.contains(c) for c in _GLOB_CHARS))
        )
    ).all()
    su = _match_source_url(list(patterns), target)
    if su is not None:
        return su.type.data, target, _concrete_object_url(su.url, target)

    candidates = session.exec(select(Data).where(Data.url != None)).all()
    matches = [
        d
        for d in candidates
        if d.source_type is None and _normalize_object_key(d.url) == target
    ]

    if not matches:
        raise HTTPException(
            status_code=404, detail=f"No source matches file_id '{file_id}'."
        )
    if len(matches) > 1:
        # Same bucket/key on multiple sources: break the tie on host+port+path.
        # If that doesn't resolve to exactly one (bare key, or unknown host), 409.
        full_target = _normalize_full_url(file_id)
        refined = [d for d in matches if _normalize_full_url(d.url) == full_target]
        if len(refined) != 1:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Ambiguous file_id '{file_id}': {len(matches)} sources "
                    "share this key and it could not be resolved by full URL."
                ),
            )
        matches = refined

    return matches[0], target, matches[0].url


def _clean_etag(value: str | None) -> str | None:
    """An etag with S3's quoting removed, or None if there isn't one.

    Blank counts as absent. A notify script whose ETAG variable is unset sends
    an empty string rather than omitting the field, and taking that as a
    checksum records a version of `""` — after which every later notify for
    that object dedups against it and silently stops triggering.
    """
    if value is None:
        return None
    return value.strip().strip('"') or None


def _change_metadata(
    etag: str | None, size: int | None, url: str | None
) -> tuple[str, int]:
    """Checksum + size for a no-copy version, HEADing the object if not supplied."""
    etag = _clean_etag(etag)

    if etag and size is not None:
        return etag, size

    if url:
        try:
            resp = requests.head(url, allow_redirects=True, timeout=30)
            if resp.status_code < 400:
                etag = etag or _clean_etag(resp.headers.get("ETag"))
                if size is None and resp.headers.get("Content-Length") is not None:
                    size = int(resp.headers["Content-Length"])
            else:
                logger.warning("HEAD %s returned %s", url, resp.status_code)
        except requests.RequestException as e:  # network/DNS/timeout
            logger.warning("HEAD %s failed: %s", url, e)

    if not etag:
        raise HTTPException(
            status_code=400,
            detail=(
                "A no-copy notify needs an etag: send one in the payload, or make "
                "the object reachable by HEAD so it can be read from the response."
            ),
        )

    return etag, size if size is not None else 0


def _run_event_ingestion(
    session: Session,
    data: Data,
    trigger_url: str | None,
    signed_url: str | None,
    object_key: str,
    etag: str | None = None,
    size: int | None = None,
    dedup: bool = True,
) -> dict:
    """Record the change to ``data`` and trigger whatever it feeds.

    A no-copy source is handled inline: the version comes straight from the event
    metadata and no bytes move. Otherwise the ``INGESTION_EVENT`` flow contributing
    to this Data runs once (immediately, no timer) and pulls the object.

    ``signed_url`` is the transient per-run url (e.g. a MinIO presigned GET) and is
    never persisted; ``trigger_url`` is the stable registered url.
    """
    if data.no_copy:
        checksum, resolved_size = _change_metadata(
            etag, size, signed_url or trigger_url
        )
        suffix = Path(object_key).suffix.lstrip(".")

        version = data.add_new_version(
            session=session,
            new_file=object_key,
            format=suffix,
            checksum=checksum,
            size=resolved_size,
            created_at=utcnow(),
            source_key=object_key,
            dedup=dedup,
        )

        if version is None:  # dedup hit: same object, same etag
            logger.info(
                "notify %s: unchanged (etag %s), no rerun", object_key, checksum
            )
            return {
                "status": "unchanged",
                "data_id": data.id,
                "source_key": object_key,
            }

        logger.info(
            "notify %s: new version for data %s, triggering dependents "
            "(trigger_url=%s signed=%s)",
            object_key,
            data.id,
            trigger_url,
            bool(signed_url),
        )
        policies = data.rerun_flow(
            session=session, trigger_url=trigger_url, signed_url=signed_url
        )
        logger.info(
            "notify %s: rerun_flow matched %d dependent flow(s), policies=%s",
            object_key,
            len(policies),
            policies,
        )
        return {
            "status": "version created",
            "data_id": data.id,
            "source_key": object_key,
            "version": version.version,
        }

    # Run the copy data path
    flow = session.exec(
        select(Flow).where(
            Flow.contributed_to.any(id=data.id),
            Flow.policy == TriggerEnum.INGESTION_EVENT,
        )
    ).first()
    if flow is None:
        raise HTTPException(
            status_code=404,
            detail=f"No event-driven ingestion flow produces data {data.id}.",
        )

    flow._run_ingestion_flow(
        session=session,
        source_url=signed_url or trigger_url,
        source_key=object_key,
        dedup=dedup,
    )
    return {"status": "ingestion triggered", "flow_id": flow.id, "data_id": data.id}


@webhook_router.post("/{id}/notify")
def notify_update(
    id: UUID,
    payload: NotifyIn | None = None,
    x_aero_token: str | None = Header(default=None),
    session: Session = Depends(get_session),
):
    """Webhook: an upstream source (e.g. an S3 object) changed.

    Runs the event-driven (INGESTION_EVENT) ingestion flow that produces this
    Data. The flow re-pulls the source and its commit function records a new
    version, which in turn triggers any dependent analysis flows.
    """
    _require_webhook_token(x_aero_token)

    d = session.exec(select(Data).where(Data.id == id)).first()
    if d is None:
        raise HTTPException(status_code=404, detail=f"Data with id {id} not found.")

    signed_url = payload.url if payload else None
    identity = (payload.key or payload.url) if payload else None

    if d.source_type is not None:
        # A typed source owns a known set of urls; work out which one fired so the
        # version records it and dedup is per-url. Unregistered urls are rejected.
        if identity is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Data {id} is type '{d.source_type.name}' with "
                    f"{len(d.source_type.urls)} registered urls — send 'key' or "
                    "'url' so the notify identifies which one changed."
                ),
            )
        target = _normalize_object_key(identity)
        match = _match_source_url(list(d.source_type.urls), target)
        if match is None:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"'{identity}' matches no registered url or pattern of type "
                    f"'{d.source_type.name}'."
                ),
            )
        # The concrete key, not the pattern that matched it -- it becomes the
        # version's source_key and drives per-object dedup.
        object_key = target
        trigger_url = _concrete_object_url(match.url, target)
    else:
        object_key = _normalize_object_key(identity or d.url or str(id))
        trigger_url = d.url

    return _run_event_ingestion(
        session=session,
        data=d,
        trigger_url=trigger_url,
        signed_url=signed_url,
        object_key=object_key,
        etag=payload.etag if payload else None,
        size=payload.size if payload else None,
        dedup=payload.dedup if payload else True,
    )


@webhook_router.post("/notify")
def notify_by_object(
    payload: NotifyByObjectIn,
    x_aero_token: str | None = Header(default=None),
    session: Session = Depends(get_session),
):
    """Webhook: notify by object identity (``file_id``) instead of Data UUID.

    Resolves the Data record whose registered ``url`` normalizes to the same
    ``bucket/key`` as ``file_id``, then runs its event-driven ingestion flow.
    Lets an S3/MinIO event pipeline trigger ingestion knowing only the object
    it changed — not AERO's internal UUID.
    """
    _require_webhook_token(x_aero_token)

    data, object_key, trigger_url = _resolve_notify_target(session, payload.file_id)

    return _run_event_ingestion(
        session=session,
        data=data,
        trigger_url=trigger_url,
        signed_url=payload.url,
        object_key=object_key,
        etag=payload.etag,
        size=payload.size,
        dedup=payload.dedup,
    )
