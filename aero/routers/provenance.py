from uuid import UUID
from datetime import datetime


from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Query

from pydantic import BaseModel

from sqlmodel import select
from sqlmodel import Session

from aero.auth import require_globus_auth
from aero.database import get_session

from aero.models.data import Data
from aero.models.data_version import DataVersion
from aero.models.provenance import Provenance
from aero.models.types import utcnow


router = APIRouter(
    prefix="/prov",
    tags=["provenance"],
    dependencies=[Depends(require_globus_auth)],
    responses={404: {"description": "Not found"}},
)


class ProvRecord(BaseModel):
    input_data:  dict[
        str, dict[str, str | bool | int | float | list[str | None] | None]
    ]
    output_data: dict[
        str, dict[str, str | bool | int | float | list[str | None] | None]
    ]
    flow_id: UUID


@router.get("/", response_model=list[Provenance])
# @authenticated
def list_prov(
    offset: int = 0,
    limit: int = Query(default=15, le=15),
    session: Session = Depends(get_session),
):
    provs = session.exec(
        select(Provenance).order_by(Provenance.id.desc()).offset(offset).limit(limit)
    ).all()
    return provs


@router.post("/new", response_model=Provenance)
# @authenticated
def add_record(pr: ProvRecord, session: Session = Depends(get_session)):
    try:
        input_versions = [
            session.exec(
                select(DataVersion).where(
                    DataVersion.data_id == UUID(i["id"]),
                    DataVersion.version == i["version"],
                )
            ).first()
            for i in pr.input_data.values()
        ]
        # create new versions for output data
        #
        # Stamp created_at on the server rather than trusting the worker's
        # value: the worker sends datetime.now().ctime(), which is its *local*
        # wall clock with no offset and no way to recover one. Taking it would
        # make a version from an EDT worker look hours older than the UTC
        # instant in Flow.last_executed, and the ANY/ALL rerun gate -- which
        # compares version.created_at > flow.last_executed -- would never fire
        # again. One clock for both keeps the comparison right wherever the
        # worker runs.
        committed_at = utcnow()
        output_versions = []
        for o in pr.output_data.values():
            d = session.exec(select(Data).where(Data.id == UUID(o["id"]))).first()
            d.add_new_version(
                session=session,
                new_file=o["file_bn"],
                format=o["file_format"],
                checksum=o["checksum"],
                size=o["size"],
                created_at=committed_at,
                encoding=o.get("encoding", "utf-8"),
                # Set by the notify for a typed source: which object this pull came
                # from, so change detection is per-url rather than per-Data. Absent
                # for untyped sources, which keeps the original tail comparison.
                source_key=o.get("source_key"),
                dedup=o.get("dedup", True),
            )

            # TODO: maybe fix
            d.rerun_flow(session=session)
            output_versions.append(d.last_version())

        p = Provenance(
            flow_id=pr.flow_id,
            contributed_to=output_versions,
            derived_from=input_versions,
        )

        session.add(p)
        session.commit()
        session.refresh(p)
        return p
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
