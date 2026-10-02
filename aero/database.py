import logging

from sqlalchemy import inspect

from sqlmodel import create_engine
from sqlmodel import SQLModel
from sqlmodel import Session

from aero.config import Config

logger = logging.getLogger(__name__)

engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)

# Columns added to tables that already existed in deployed databases. create_all()
# creates missing *tables* but never alters existing ones, so on an upgraded database
# these have to be added by hand — and the failure is otherwise silent until a query
# hits them.
_REQUIRED_COLUMNS = {
    "data": ["no_copy"],
    "dataversion": ["source_key"],
}

_MIGRATION_SQL = """\
ALTER TABLE data        ADD COLUMN no_copy    boolean NOT NULL DEFAULT false;
ALTER TABLE dataversion ADD COLUMN source_key varchar;"""

# Timestamps are aware UTC instants (aero.models.types.UTCDateTime). create_all()
# does not alter an existing column's type either, and a database still holding
# these as `timestamp without time zone` rejects every write to them — so say so
# at startup rather than at the first ingestion.
_UTC_COLUMNS = {
    "dataversion": ["created_at"],
    "flow": ["last_executed"],
    "sourcetype": ["created_at"],
    "sourceurl": ["created_at"],
}

_UTC_MIGRATION = "scripts/migrate_utc_timestamps.sql"


def create_db_and_tables():
    SQLModel.metadata.create_all(engine)
    check_schema()


def check_schema() -> list[str]:
    """Warn loudly about schema drift create_all() cannot fix by itself.

    Covers columns missing from tables that already existed, and timestamps still
    stored without a timezone.

    Returns the list of offending "table.column" names (empty when the schema is
    good).
    """
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())

    missing = []
    for table, columns in _REQUIRED_COLUMNS.items():
        if table not in tables:
            continue  # create_all just built it, or it is genuinely absent
        present = {c["name"] for c in inspector.get_columns(table)}
        missing.extend(f"{table}.{c}" for c in columns if c not in present)

    if missing:
        logger.error(
            "Database schema is out of date — missing %s. create_all() cannot add "
            "columns to existing tables; apply this by hand (adminer) and restart:\n%s",
            ", ".join(missing),
            _MIGRATION_SQL,
        )

    naive = _naive_timestamp_columns(inspector, tables)
    if naive:
        logger.error(
            "Database schema is out of date — %s still store timestamps without a "
            "timezone, and every write to them will fail. Apply %s and restart.",
            ", ".join(naive),
            _UTC_MIGRATION,
        )

    return missing + naive


def _naive_timestamp_columns(inspector, tables: set[str]) -> list[str]:
    """The timestamp columns that should be aware but are not.

    Only meaningful on Postgres: SQLite has no aware column type, and the
    UTCDateTime decorator is what makes it behave as though it did.
    """
    if engine.dialect.name != "postgresql":
        return []

    naive = []
    for table, columns in _UTC_COLUMNS.items():
        if table not in tables:
            continue
        types = {c["name"]: c["type"] for c in inspector.get_columns(table)}
        for column in columns:
            found = types.get(column)
            if found is not None and getattr(found, "timezone", True) is False:
                naive.append(f"{table}.{column}")

    return naive


def get_session():
    with Session(engine) as session:
        yield session
