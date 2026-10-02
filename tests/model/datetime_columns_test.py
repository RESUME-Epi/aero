"""Every stored timestamp is naive, in the server's local clock.

sqlmodel infers a *timezone-aware* column from a bare ``datetime`` annotation and
then refuses to bind the naive values this schema writes, so each field spells out
``sa_type=DateTime``. These assert the resulting columns, because a version of
sqlmodel without that inference maps either spelling to the same thing and the
mistake only shows up against the deployed database.

Aware columns would also relabel the rows already stored -- written on the server's
local clock -- as UTC on read, which shifts ``DataVersion.created_at`` relative to
``Flow.last_executed`` and breaks the ANY/ALL rerun gate.
"""

import pytest

from aero.models.data_version import DataVersion
from aero.models.flows import Flow
from aero.models.source_type import SourceType
from aero.models.source_type import SourceUrl


@pytest.mark.parametrize(
    "model, column",
    [
        (SourceType, "created_at"),
        (SourceUrl, "created_at"),
        (DataVersion, "created_at"),
        (Flow, "last_executed"),
    ],
)
def test_timestamp_columns_are_naive(model, column):
    col = model.__table__.columns[column]
    assert col.type.timezone is False, (
        f"{model.__tablename__}.{column} is timezone-aware; the stored rows are "
        "naive server-local, so reading them as UTC would shift them"
    )


@pytest.mark.parametrize(
    "model, column",
    [
        (SourceType, "created_at"),
        (SourceUrl, "created_at"),
        (DataVersion, "created_at"),
        (Flow, "last_executed"),
    ],
)
def test_timestamp_columns_compile_without_a_timezone(model, column):
    """The DDL itself, so a type decorator can't hide an aware impl."""
    from sqlalchemy.dialects import postgresql

    col = model.__table__.columns[column]
    assert col.type.compile(postgresql.dialect()) == "TIMESTAMP WITHOUT TIME ZONE"
