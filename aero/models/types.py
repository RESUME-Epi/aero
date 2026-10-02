"""Column types shared by the models."""

from datetime import datetime
from datetime import timezone

from sqlalchemy import types
from sqlalchemy.engine.interfaces import Dialect


class UTCDateTime(types.TypeDecorator):
    """A timestamp stored and returned as an aware UTC ``datetime``.

    Every timestamp in this schema is an instant, so it is kept as one. Binding
    a naive value raises rather than guessing which zone it meant: a naive local
    time is not an instant, and it is not monotonic either — across a DST
    fall-back the same wall clock happens twice, which would let
    ``DataVersion.created_at > Flow.last_executed`` read false for a version
    created after the run and silently stop an ANY/ALL analysis rerunning.

    Backends that cannot hold an offset (SQLite) store UTC without one, so
    reading attaches UTC rather than handing back a naive value — which keeps
    the tests comparing the same kinds of thing the deployed Postgres does.
    """

    impl = types.DateTime
    cache_ok = True

    def __init__(self, *args, **kwargs):
        kwargs["timezone"] = True
        super().__init__(*args, **kwargs)

    def process_bind_param(
        self, value: datetime | None, dialect: Dialect
    ) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError(
                "Timestamps must be timezone-aware. Use "
                "datetime.now(timezone.utc) rather than datetime.now()."
            )
        return value.astimezone(timezone.utc)

    def process_result_value(
        self, value: datetime | None, dialect: Dialect
    ) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


def utcnow() -> datetime:
    """The current instant, as an aware UTC ``datetime``."""
    return datetime.now(timezone.utc)
