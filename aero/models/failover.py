"""Alternate Globus Compute targets for a flow whose endpoint is down.

Failover is per-flow rather than per-endpoint: two analyses pinned to the same
primary endpoint can name different fallbacks, because what makes a fallback
valid is whether it can run *that* analysis — the right profile, the right
Python, reachability to the object store it reads.

A flow with no rows here is never status-checked, so the cost of the mechanism
is only paid where one is configured.
"""

from uuid import UUID
from uuid import uuid4

from typing import TYPE_CHECKING
from typing import Optional

from sqlmodel import Field
from sqlmodel import Relationship
from sqlmodel import SQLModel


if TYPE_CHECKING:  # pragma: nocover
    from aero.models.flows import Flow


class FlowFailover(SQLModel, table=True):
    __tablename__ = "flowfailover"

    id: UUID = Field(default_factory=uuid4, index=True, primary_key=True)
    flow_id: UUID = Field(foreign_key="flow.id", index=True, nullable=False)
    # Ascending try order. Unique per flow, so the chain is unambiguous.
    priority: int = Field(default=0, nullable=False)
    endpoint: UUID = Field(nullable=False)
    # None means "reuse the primary's". Set these when the failover endpoint runs
    # a different Python minor version: a function registered against one does not
    # deserialize on the other, so it has to be re-registered and gets a new uuid.
    function_id: UUID | None = Field(default=None)
    pull_function_id: UUID | None = Field(default=None)
    commit_function_id: UUID | None = Field(default=None)
    enabled: bool = Field(default=True, nullable=False)

    flow: Optional["Flow"] = Relationship(back_populates="failovers")
