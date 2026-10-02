"""Notification source types.

A *type* groups several source URLs under a single ``Data`` UUID. Analysis flows
register against that one UUID, so a change to any of the type's URLs triggers the
same analysis — and the run is told which URL fired.

``SourceUrl.object_key`` holds the normalized object key (see
``aero.routers.data._normalize_object_key``) so a notify resolves with an indexed
exact lookup instead of scanning every ``Data.url``. It is also the per-URL dedup
key recorded on each ``DataVersion.source_key``.
"""

from uuid import UUID
from uuid import uuid4
from datetime import datetime

from typing import TYPE_CHECKING
from typing import Optional

from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel

from aero.models.types import UTCDateTime
from aero.models.types import utcnow


if TYPE_CHECKING:  # pragma: nocover
    from aero.models.data import Data


class SourceType(SQLModel, table=True):
    __tablename__ = "sourcetype"

    id: UUID = Field(default_factory=uuid4, index=True, primary_key=True)
    name: str = Field(index=True, unique=True)
    data_id: UUID = Field(foreign_key="data.id", unique=True)
    created_at: datetime = Field(default_factory=utcnow, sa_type=UTCDateTime)

    data: Optional["Data"] = Relationship(back_populates="source_type")
    urls: list["SourceUrl"] = Relationship(
        back_populates="type",
        sa_relationship_kwargs={"cascade": "all, delete-orphan"},
    )


class SourceUrl(SQLModel, table=True):
    __tablename__ = "sourceurl"

    id: UUID = Field(default_factory=uuid4, index=True, primary_key=True)
    type_id: UUID = Field(foreign_key="sourcetype.id")
    url: str = Field(nullable=False)
    # _normalize_object_key(url): scheme/host/query stripped. The match + dedup key.
    object_key: str = Field(index=True, unique=True)
    created_at: datetime = Field(default_factory=utcnow, sa_type=UTCDateTime)

    type: Optional["SourceType"] = Relationship(back_populates="urls")
