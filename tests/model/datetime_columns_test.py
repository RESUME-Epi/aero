"""Every stored timestamp is an aware UTC instant.

The columns are declared with ``UTCDateTime`` rather than letting sqlmodel infer
a type, because only some sqlmodel releases read a bare ``datetime`` annotation
as timezone-aware — so the inferred column would depend on which version the
image happened to install.

What the decorator buys beyond the annotation is uniform behaviour across
backends: SQLite cannot hold an offset, so without it the tests would round-trip
naive values while the deployed Postgres round-trips aware ones, and the
comparison in the ANY/ALL rerun gate would be exercised in a shape production
never sees.
"""

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from uuid import uuid4

import pytest

from sqlalchemy.exc import StatementError
from sqlmodel import select

import aero.models.data
import aero.models.data_version

from aero.models.data_version import DataVersion
from aero.models.flows import Flow
from aero.models.source_type import SourceType
from aero.models.source_type import SourceUrl
from aero.models.types import UTCDateTime
from aero.models.types import utcnow


COLUMNS = [
    (SourceType, "created_at"),
    (SourceUrl, "created_at"),
    (DataVersion, "created_at"),
    (Flow, "last_executed"),
]


@pytest.mark.parametrize("model, column", COLUMNS)
def test_timestamp_columns_are_utc(model, column):
    col = model.__table__.columns[column]
    assert isinstance(col.type, UTCDateTime), (
        f"{model.__tablename__}.{column} does not use UTCDateTime, so what it "
        "returns depends on the backend and the sqlmodel version"
    )
    assert col.type.timezone is True


@pytest.mark.parametrize("model, column", COLUMNS)
def test_timestamp_columns_compile_with_a_timezone(model, column):
    """The DDL, so the migration and the models can be checked against each other."""
    from sqlalchemy.dialects import postgresql

    col = model.__table__.columns[column]
    assert col.type.compile(postgresql.dialect()) == "TIMESTAMP WITH TIME ZONE"


def test_a_naive_write_is_refused(session):
    """Better a loud failure than a wall clock silently recorded as an instant.

    SQLAlchemy wraps the decorator's ValueError in a StatementError on the way
    out of the flush, which is the shape the error actually reaches a caller in.
    """
    d = aero.models.data.create_data(
        session=session, name="d", url="http://minio/d.csv",
        collection_url="https://globus.org/test", collection_uuid=uuid4(),
        description="",
    )
    v = aero.models.data_version.create_dataversion(
        session=session, version=1, checksum="c", data_id=d.id
    )

    v.created_at = datetime(2026, 8, 14, 12, 0, 0)
    session.add(v)

    with pytest.raises(StatementError, match="timezone-aware") as excinfo:
        session.commit()

    assert isinstance(excinfo.value.orig, ValueError)


def test_a_stored_timestamp_comes_back_aware(session):
    """SQLite drops the offset on write; reading has to put it back."""
    d = aero.models.data.create_data(
        session=session, name="d", url="http://minio/d.csv",
        collection_url="https://globus.org/test", collection_uuid=uuid4(),
        description="",
    )
    written = datetime(2026, 8, 14, 12, 0, 0, tzinfo=timezone.utc)
    v = aero.models.data_version.create_dataversion(
        session=session, version=1, checksum="c", data_id=d.id
    )
    v.created_at = written
    session.add(v)
    session.commit()
    session.expire_all()

    loaded = session.exec(select(DataVersion).where(DataVersion.id == v.id)).first()
    assert loaded.created_at.tzinfo is not None
    assert loaded.created_at == written


def test_a_non_utc_write_is_stored_as_the_same_instant(session):
    """An offset other than UTC is converted, not truncated."""
    d = aero.models.data.create_data(
        session=session, name="d", url="http://minio/d.csv",
        collection_url="https://globus.org/test", collection_uuid=uuid4(),
        description="",
    )
    eastern = timezone(timedelta(hours=-4))
    written = datetime(2026, 8, 14, 8, 0, 0, tzinfo=eastern)
    v = aero.models.data_version.create_dataversion(
        session=session, version=1, checksum="c", data_id=d.id
    )
    v.created_at = written
    session.add(v)
    session.commit()
    session.expire_all()

    loaded = session.exec(select(DataVersion).where(DataVersion.id == v.id)).first()
    assert loaded.created_at == written
    assert loaded.created_at == datetime(2026, 8, 14, 12, 0, 0, tzinfo=timezone.utc)


def test_utcnow_is_aware():
    assert utcnow().tzinfo is not None
