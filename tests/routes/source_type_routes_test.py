"""Typed notification sources: many urls -> one type -> one Data UUID."""

from uuid import UUID
from uuid import uuid4

import pytest

from sqlmodel import select

import aero.models.data
import aero.models.function

from aero.models.data import Data


TYPES = "/data/types"
SOURCE = "/data/source"
NOTIFY = "/data/notify"

URL_A = "http://127.0.0.1:9000/traffic/a.xml.gz"
URL_B = "http://127.0.0.1:9000/traffic/b.xml.gz"


def _create_typed_source(client, name="traffic", url=URL_A, no_copy=True):
    resp = client.post(
        SOURCE,
        json={"name": name, "url": url, "type": name, "no_copy": no_copy},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def test_create_typed_no_copy_source(client, session):
    body = _create_typed_source(client)

    assert body["no_copy"] is True
    assert body["url"] == URL_A

    d = session.exec(select(Data).where(Data.id == UUID(body["id"]))).first()
    assert d.source_type is not None
    assert d.source_type.name == "traffic"
    assert [u.object_key for u in d.source_type.urls] == ["traffic/a.xml.gz"]


def test_second_url_reuses_the_same_data_uuid(client):
    body = _create_typed_source(client)

    resp = client.post(f"{TYPES}/traffic/urls", json={"url": URL_B})
    assert resp.status_code == 200, resp.text

    out = resp.json()
    # The whole point: another url, same Data UUID, so analysis flows registered
    # against it now fire for this object too.
    assert out["data_id"] == body["id"]
    assert sorted(u["object_key"] for u in out["urls"]) == [
        "traffic/a.xml.gz",
        "traffic/b.xml.gz",
    ]


def test_duplicate_object_key_across_types_is_rejected(client):
    _create_typed_source(client, name="traffic", url=URL_A)
    _create_typed_source(client, name="other", url=URL_B)

    # a.xml.gz already belongs to 'traffic'
    resp = client.post(f"{TYPES}/other/urls", json={"url": URL_A})
    assert resp.status_code == 409, resp.text
    assert "traffic" in resp.json()["detail"]


def test_re_registering_the_same_url_is_idempotent(client):
    _create_typed_source(client)
    resp = client.post(f"{TYPES}/traffic/urls", json={"url": URL_A})
    assert resp.status_code == 200, resp.text
    assert len(resp.json()["urls"]) == 1


def test_list_and_get_types(client):
    _create_typed_source(client)

    resp = client.get(TYPES)
    assert resp.status_code == 200, resp.text
    assert [t["name"] for t in resp.json()] == ["traffic"]

    resp = client.get(f"{TYPES}/traffic")
    assert resp.status_code == 200, resp.text
    assert resp.json()["no_copy"] is True

    assert client.get(f"{TYPES}/nope").status_code == 404


def test_types_route_is_not_shadowed_by_the_uuid_route(client):
    """/data/types must be declared before /data/{id}, or it 422s as a bad UUID."""
    assert client.get(TYPES).status_code == 200


def test_create_type_twice_conflicts(client):
    _create_typed_source(client)
    resp = client.post(
        SOURCE,
        json={"name": "traffic", "url": URL_B, "type": "traffic", "no_copy": True},
    )
    assert resp.status_code == 409, resp.text


# --------------------------------------------------------------------------
# removing a url
# --------------------------------------------------------------------------


def test_delete_url_leaves_the_type_and_its_other_urls(client, session):
    body = _create_typed_source(client)
    client.post(f"{TYPES}/traffic/urls", json={"url": URL_B})

    resp = client.delete(f"{TYPES}/traffic/urls", params={"url": URL_A})
    assert resp.status_code == 200, resp.text

    out = resp.json()
    assert out["data_id"] == body["id"]
    assert [u["object_key"] for u in out["urls"]] == ["traffic/b.xml.gz"]


def test_delete_url_normalizes_what_it_is_given(client):
    """The caller should not have to reproduce the registered form exactly."""
    _create_typed_source(client)

    presigned = f"{URL_A}?X-Amz-Signature=abc&X-Amz-Expires=3600"
    resp = client.delete(f"{TYPES}/traffic/urls", params={"url": presigned})

    assert resp.status_code == 200, resp.text
    assert resp.json()["urls"] == []


def test_delete_url_belonging_to_another_type_conflicts(client):
    _create_typed_source(client, name="traffic", url=URL_A)
    _create_typed_source(client, name="other", url=URL_B)

    resp = client.delete(f"{TYPES}/other/urls", params={"url": URL_A})
    assert resp.status_code == 409, resp.text
    assert "traffic" in resp.json()["detail"]

    # and the url is still where it was
    assert len(client.get(f"{TYPES}/traffic").json()["urls"]) == 1


def test_delete_unregistered_url_404s(client):
    _create_typed_source(client)
    resp = client.delete(f"{TYPES}/traffic/urls", params={"url": URL_B})
    assert resp.status_code == 404, resp.text


def test_delete_url_from_unknown_type_404s(client):
    resp = client.delete(f"{TYPES}/nope/urls", params={"url": URL_A})
    assert resp.status_code == 404, resp.text


def test_removing_the_last_url_makes_notify_unresolvable(client, session):
    """Nothing is destroyed, but there is no longer any way in.

    Resolution falls back to untyped sources only, so a type with no urls cannot
    be reached even though its Data.url still holds the url it was created with.
    """
    body = _create_typed_source(client)
    client.post(NOTIFY, json={"file_id": URL_A, "etag": "aaa", "size": 1})

    resp = client.delete(f"{TYPES}/traffic/urls", params={"url": URL_A})
    assert resp.status_code == 200, resp.text
    assert resp.json()["urls"] == []

    assert client.post(NOTIFY, json={"file_id": URL_A, "etag": "bbb"}).status_code == 404

    d = session.exec(select(Data).where(Data.id == UUID(body["id"]))).first()
    assert d.source_type is not None
    assert d.url == URL_A
    assert [v.source_key for v in d.versions] == ["traffic/a.xml.gz"]


def test_a_removed_url_can_be_registered_again(client):
    body = _create_typed_source(client)
    client.delete(f"{TYPES}/traffic/urls", params={"url": URL_A})

    resp = client.post(f"{TYPES}/traffic/urls", json={"url": URL_A})
    assert resp.status_code == 200, resp.text
    assert resp.json()["data_id"] == body["id"]

    ok = client.post(NOTIFY, json={"file_id": URL_A, "etag": "aaa", "size": 1})
    assert ok.status_code == 200, ok.text


def test_a_removed_url_is_free_for_another_type(client):
    _create_typed_source(client, name="traffic", url=URL_A)
    _create_typed_source(client, name="other", url=URL_B)

    client.delete(f"{TYPES}/traffic/urls", params={"url": URL_A})

    resp = client.post(f"{TYPES}/other/urls", json={"url": URL_A})
    assert resp.status_code == 200, resp.text
    assert sorted(u["object_key"] for u in resp.json()["urls"]) == [
        "traffic/a.xml.gz",
        "traffic/b.xml.gz",
    ]


# --------------------------------------------------------------------------
# notify
# --------------------------------------------------------------------------


def test_notify_resolves_presigned_url_to_registered_key(client, session):
    body = _create_typed_source(client)
    presigned = f"{URL_A}?X-Amz-Signature=abc&X-Amz-Expires=3600"

    resp = client.post(
        NOTIFY,
        json={"file_id": presigned, "url": presigned, "etag": "aaa", "size": 10},
    )

    assert resp.status_code == 200, resp.text
    out = resp.json()
    assert out["status"] == "version created"
    assert out["data_id"] == body["id"]
    assert out["source_key"] == "traffic/a.xml.gz"

    d = session.exec(select(Data).where(Data.id == UUID(body["id"]))).first()
    version = d.last_version()
    assert version.checksum == "aaa"
    assert version.source_key == "traffic/a.xml.gz"
    # no-copy: the version records the object key, nothing was staged to a collection
    assert version.data_file.file_name == "traffic/a.xml.gz"
    assert d.collection_url is None


def test_notify_unregistered_url_404s(client):
    _create_typed_source(client)

    resp = client.post(
        NOTIFY,
        json={"file_id": "traffic/never-registered.xml.gz", "etag": "zzz"},
    )
    assert resp.status_code == 404, resp.text


def test_two_urls_feed_one_data(client, session):
    body = _create_typed_source(client)
    client.post(f"{TYPES}/traffic/urls", json={"url": URL_B})

    client.post(NOTIFY, json={"file_id": URL_A, "etag": "aaa", "size": 1})
    client.post(NOTIFY, json={"file_id": URL_B, "etag": "bbb", "size": 2})

    d = session.exec(select(Data).where(Data.id == UUID(body["id"]))).first()
    assert len(d.versions) == 2
    assert sorted(v.source_key for v in d.versions) == [
        "traffic/a.xml.gz",
        "traffic/b.xml.gz",
    ]


def test_dedup_is_per_url(client, session):
    """A,B,A with A unchanged makes no third version.

    Comparing against the tail of the version list (the old behavior) would see
    B's checksum and wrongly treat the repeat of A as a change.
    """
    body = _create_typed_source(client)
    client.post(f"{TYPES}/traffic/urls", json={"url": URL_B})

    client.post(NOTIFY, json={"file_id": URL_A, "etag": "aaa", "size": 1})
    client.post(NOTIFY, json={"file_id": URL_B, "etag": "bbb", "size": 2})
    resp = client.post(NOTIFY, json={"file_id": URL_A, "etag": "aaa", "size": 1})

    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "unchanged"

    d = session.exec(select(Data).where(Data.id == UUID(body["id"]))).first()
    assert len(d.versions) == 2


def test_dedup_false_forces_a_version(client, session):
    body = _create_typed_source(client)

    client.post(NOTIFY, json={"file_id": URL_A, "etag": "aaa", "size": 1})
    resp = client.post(
        NOTIFY, json={"file_id": URL_A, "etag": "aaa", "size": 1, "dedup": False}
    )

    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "version created"

    d = session.exec(select(Data).where(Data.id == UUID(body["id"]))).first()
    assert len(d.versions) == 2


def test_a_real_change_to_the_same_url_versions(client, session):
    body = _create_typed_source(client)

    client.post(NOTIFY, json={"file_id": URL_A, "etag": "aaa", "size": 1})
    client.post(NOTIFY, json={"file_id": URL_A, "etag": "ccc", "size": 3})

    d = session.exec(select(Data).where(Data.id == UUID(body["id"]))).first()
    assert len(d.versions) == 2
    assert [v.version for v in sorted(d.versions, key=lambda v: v.version)] == [1, 2]


@pytest.mark.parametrize("etag", [None, "", "   ", '""'])
def test_no_copy_notify_without_a_usable_etag_is_rejected(client, session, etag):
    """Blank counts as absent, not as a checksum of "".

    A notify script with an unset ETAG variable sends an empty string rather
    than omitting the field. Accepting it records a version whose checksum is
    empty, and every later notify for that object then dedups against it and
    silently stops triggering.
    """
    _create_typed_source(client)

    payload = {"file_id": URL_A}
    if etag is not None:
        payload["etag"] = etag

    # unreachable host, so the HEAD fallback cannot supply one either
    resp = client.post(NOTIFY, json=payload)

    assert resp.status_code == 400, resp.text
    assert "etag" in resp.json()["detail"]
    d = session.exec(select(Data).where(Data.url == URL_A)).first()
    assert d.versions == [], "a rejected notify must not leave a version behind"


def test_a_quoted_etag_still_works(client, session):
    """S3 wraps etags in quotes; only a *blank* one is treated as missing."""
    body = _create_typed_source(client)

    resp = client.post(NOTIFY, json={"file_id": URL_A, "etag": '"aaa"', "size": 1})

    assert resp.status_code == 200, resp.text
    d = session.exec(select(Data).where(Data.id == UUID(body["id"]))).first()
    assert d.last_version().checksum == "aaa"


def test_quoted_etag_is_unwrapped(client, session):
    """S3 returns ETags wrapped in quotes; dedup must not see \"x\" != x."""
    body = _create_typed_source(client)

    client.post(NOTIFY, json={"file_id": URL_A, "etag": '"aaa"', "size": 1})
    resp = client.post(NOTIFY, json={"file_id": URL_A, "etag": "aaa", "size": 1})

    assert resp.json()["status"] == "unchanged"
    d = session.exec(select(Data).where(Data.id == UUID(body["id"]))).first()
    assert len(d.versions) == 1


def test_notify_by_id_requires_a_registered_url_for_typed_data(client):
    body = _create_typed_source(client)
    data_id = body["id"]

    ok = client.post(
        f"/data/{data_id}/notify", json={"key": "traffic/a.xml.gz", "etag": "aaa"}
    )
    assert ok.status_code == 200, ok.text

    bad = client.post(
        f"/data/{data_id}/notify", json={"key": "traffic/nope.xml.gz", "etag": "x"}
    )
    assert bad.status_code == 404, bad.text

    # several urls could have fired; the payload has to say which
    client.post(f"{TYPES}/traffic/urls", json={"url": URL_B})
    vague = client.post(f"/data/{data_id}/notify", json={"etag": "x"})
    assert vague.status_code == 400, vague.text


def test_untyped_sources_are_untouched(client, session, spy):
    """The legacy Data.url scan still resolves sources that have no type."""
    import aero.models.data

    d = aero.models.data.create_data(
        session=session,
        name="legacy",
        url="http://127.0.0.1:9000/old-bucket/legacy.csv",
        collection_url="https://globus.org/test",
        collection_uuid=uuid4(),
        description="",
    )

    resp = client.post(NOTIFY, json={"file_id": "old-bucket/legacy.csv"})

    assert resp.status_code == 200, resp.text
    assert spy.calls[0]["data"].id == d.id


def test_typed_data_is_skipped_by_the_legacy_scan(client, session):
    """A typed source resolves via sourceurl, so it can't also match on Data.url."""
    body = _create_typed_source(client)

    resp = client.post(NOTIFY, json={"file_id": URL_A, "etag": "aaa", "size": 1})
    assert resp.status_code == 200, resp.text
    assert resp.json()["data_id"] == body["id"]


# --------------------------------------------------------------------------
# /latest carries what a no-copy pull needs (§7b)
# --------------------------------------------------------------------------


def test_latest_resolves_trigger_url_for_a_no_copy_version(client):
    body = _create_typed_source(client)
    client.post(NOTIFY, json={"file_id": URL_A, "etag": "aaa", "size": 1})

    resp = client.get(f"/data/{body['id']}/latest")
    assert resp.status_code == 200, resp.text

    out = resp.json()
    assert out["no_copy"] is True
    assert out["source_key"] == "traffic/a.xml.gz"
    # An analysis run that this object did not trigger can still locate it.
    assert out["trigger_url"] == URL_A


@pytest.fixture(name="spy")
def spy_fixture(monkeypatch):
    """Stub the copy-path ingestion so untyped tests don't need a real flow."""
    import aero.routers.data as data_router

    calls = []
    real = data_router._run_event_ingestion

    def _wrapped(session, data, *args, **kwargs):
        calls.append({"data": data, "args": args, "kwargs": kwargs})
        if data.no_copy:
            return real(session, data, *args, **kwargs)
        return {"status": "ingestion triggered", "data_id": data.id}

    monkeypatch.setattr(data_router, "_run_event_ingestion", _wrapped)
    _wrapped.calls = calls
    return _wrapped


def test_first_notify_runs_the_analysis(client, session, monkeypatch):
    """The registration-time skip must not swallow the first real trigger.

    Regression: the skip was keyed on `last_executed is None`, but skipping is
    exactly what leaves it None — so every later notify matched too and a
    no-copy analysis could never run at all.
    """
    import aero.models.flows as flows_model

    runs = []
    monkeypatch.setattr(
        flows_model.GLOBUS_CLIENT, "run_flow", lambda **kw: runs.append(kw)
    )

    source = _create_typed_source(client)

    # an ANY analysis reading the no-copy source
    out = aero.models.data.create_data(
        session=session, name="summary", url=None,
        collection_url="https://globus.org/test", collection_uuid=uuid4(),
        description="",
    )
    src = session.exec(select(Data).where(Data.id == UUID(source["id"]))).first()
    flow = flows_model.create_flow(
        session=session,
        derived_from=[src],
        contributed_to=[out],
        endpoint=uuid4(),
        function_id=aero.models.function.create_function(session=session, uuid=uuid4()).id,
        pull_function_id=aero.models.function.create_function(session=session, uuid=uuid4()).id,
        commit_function_id=aero.models.function.create_function(session=session, uuid=uuid4()).id,
        policy=flows_model.TriggerEnum.ANY_INPUT,
        # create_flow wraps this into {"kwargs": ..., "function": ..., "endpoint": ...}
        function_args={
            "aero": {"input_data": {"report": {"id": source["id"], "version": None}}}
        },
    )

    assert runs == [], "registration must not run a no-copy analysis"
    assert flow.last_executed is None

    client.post(NOTIFY, json={"file_id": URL_A, "etag": "aaa", "size": 1})

    assert len(runs) == 1, "the first notify must run the analysis"
    entry = runs[0]["tasks"]["kwargs"]["aero"]["input_data"]["report"]
    assert runs[0]["trigger_url"] == URL_A
    assert entry["id"] == source["id"]


def test_search_failure_does_not_suppress_the_analysis(client, session, monkeypatch):
    """A denied Search ingest must not be mistaken for "nothing changed".

    Regression (found live): add_search_entry returns the Globus error payload on
    failure -- a dict, exactly what add_new_version used to return for a dedup
    hit. The notify branched on the type, reported "unchanged", and never called
    rerun_flow. Versions accumulated while the analysis never ran.
    """
    import aero.models.data as data_model
    import aero.models.flows as flows_model

    monkeypatch.setattr(
        data_model.GLOBUS_CLIENT,
        "add_search_entry",
        lambda entry: {
            "status": 403,
            "code": "Forbidden.Generic",
            "message": "ingest request denied by service",
        },
    )
    runs = []
    monkeypatch.setattr(
        flows_model.GLOBUS_CLIENT, "run_flow", lambda **kw: runs.append(kw)
    )

    source = _create_typed_source(client)
    out = aero.models.data.create_data(
        session=session, name="summary", url=None,
        collection_url="https://globus.org/test", collection_uuid=uuid4(), description="",
    )
    mk = lambda: aero.models.function.create_function(session=session, uuid=uuid4()).id
    src = session.exec(select(Data).where(Data.id == UUID(source["id"]))).first()
    flows_model.create_flow(
        session=session, derived_from=[src], contributed_to=[out], endpoint=uuid4(),
        function_id=mk(), pull_function_id=mk(), commit_function_id=mk(),
        policy=flows_model.TriggerEnum.ANY_INPUT,
        function_args={
            "aero": {"input_data": {"report": {"id": source["id"], "version": None}}}
        },
    )

    resp = client.post(NOTIFY, json={"file_id": URL_A, "etag": "aaa", "size": 1})

    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "version created", resp.json()
    assert len(runs) == 1, "the analysis must still run when indexing fails"
