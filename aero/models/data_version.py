from datetime import datetime
from uuid import UUID
from uuid import uuid4
from typing import TYPE_CHECKING
from typing import Optional

from sqlalchemy import DateTime
from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import Session
from sqlmodel import SQLModel

from aero.models.data_file import DataFile
from aero.models.provenance import ProvenanceContribution
from aero.models.provenance import ProvenanceDerivation

if TYPE_CHECKING:  # pragma: nocover
    from aero.models.data import Data
    from aero.models.provenance import Provenance


class DataVersion(SQLModel, table=True):
    __tablename__ = "dataversion"
    id: UUID = Field(default_factory=uuid4, index=True, primary_key=True, unique=True)
    version: int | None = Field(default=None, index=True)
    checksum: str | None = Field(default=None)
    # Naive, in the server's local time -- see the note on sa_type in
    # aero.models.flows. The ANY/ALL rerun gate compares this against
    # Flow.last_executed, so the two must be stamped from the same clock.
    created_at: datetime = Field(default_factory=datetime.now, sa_type=DateTime)
    # Normalized object key this version came from. NULL for versions that predate
    # typed sources, which keeps their dedup on the old tail-comparison path.
    source_key: str | None = Field(default=None, index=True)
    data_id: Optional[UUID] = Field(
        default=None, foreign_key="data.id", primary_key=True
    )  # Column(Uuid, db.ForeignKey("data.id"))
    data: Optional["Data"] = Relationship(back_populates="versions")
    data_file: Optional["DataFile"] = Relationship(back_populates="version")
    provenance_contribution: Optional["Provenance"] = Relationship(
        link_model=ProvenanceContribution, back_populates="contributed_to"
    )
    provenance_source: Optional["Provenance"] = Relationship(
        link_model=ProvenanceDerivation, back_populates="derived_from"
    )

    def _conf_search_entry(self) -> dict:
        entry = {
            "ingest_type": "GMetaEntry",
            "ingest_data": {
                "subject": f"{self.data.name}-{self.data.id}.{self.version}",
                "visible_to": ["public"],
                "content": {
                    "name": self.data.name,
                    "description": self.data.description,
                    "created_by": dict(self.data),
                    "tags": [t for t in self.data.tags],
                    "source": self.data.url,
                    "data_id": self.data.id,
                    "version_id": self.id,
                    "version": self.version,
                    "checksum": self.checksum,
                    "file_size": (
                        str(self.data_file.size) if self.data_file is not None else None
                    ),
                    "created": (
                        self.created_at.strftime("%Y/%m/%d")
                        if self.created_at is not None
                        else None
                    ),
                    "url": self._entry_url(),
                },
            },
        }

        return entry

    def _entry_url(self) -> str | None:
        """Where the bytes for this version live.

        A no-copy version has no collection and no stored file, so fall back to the
        object it references. Guarding data_file also fixes an AttributeError for any
        version created without one (e.g. via ``create_dataversion``).
        """
        if self.data.collection_url and self.data_file is not None:
            return f"{self.data.collection_url}/{self.data_file.file_name}"
        return self.source_key or self.data.url


def create_dataversion(
    session: Session,
    version: int,
    checksum: str,
    data_id: UUID,
) -> DataVersion:
    v = DataVersion(
        version=version,
        checksum=checksum,
        data_id=data_id,
    )

    session.add(v)
    session.commit()
    session.refresh(v)

    return v
