import logging

from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum
from uuid import UUID
from uuid import uuid4
from typing import TYPE_CHECKING
from typing import Optional

from sqlmodel import Column
from sqlmodel import Field
from sqlmodel import JSON
from sqlmodel import Relationship
from sqlmodel import Session
from sqlmodel import SQLModel

from aero import GLOBUS_CLIENT
from aero.config import Config
from aero.models.error import FLOW_TIMER_ERROR
from aero.models.error import ServiceError
from aero.globus.health import endpoint_is_up
from aero.globus.utils import FlowEnum
from aero.models.failover import FlowFailover
from aero.models.function import Function
from aero.models.types import UTCDateTime
from aero.models.types import utcnow


logger = logging.getLogger(__name__)


if TYPE_CHECKING:  # pragma: nocover
    from aero.models.data import Data


class TriggerEnum(IntEnum):
    NONE = -1
    INGESTION = 0
    TIMER = 1
    ANY_INPUT = 2
    ALL_INPUT = 3
    INGESTION_EVENT = 4  # ingestion flow run on external notification, no timer


class FlowDerivation(SQLModel, table=True):
    __tablename__ = "flowderivation"
    flow_id: Optional[UUID] = Field(
        default=None, foreign_key="flow.id", primary_key=True
    )
    previous_data_id: Optional[UUID] = Field(
        default=None, foreign_key="data.id", primary_key=True
    )


class FlowContribution(SQLModel, table=True):
    __tablename__ = "flowcontribution"
    flow_id: Optional[UUID] = Field(
        default=None, foreign_key="flow.id", primary_key=True
    )
    produced_data_id: Optional[UUID] = Field(
        default=None, foreign_key="data.id", primary_key=True
    )


@dataclass(frozen=True)
class RunTarget:
    """Where one run goes: an endpoint and the three functions to run on it.

    Resolved per run and passed down to the submit call. It is never persisted —
    the flow keeps naming its primary, so a failover run does not quietly become
    the flow's new home.
    """

    endpoint: UUID | None
    function_id: UUID | None
    pull_function_id: UUID | None
    commit_function_id: UUID | None
    is_failover: bool = False
    reason: str | None = None


class Flow(SQLModel, table=True):
    __tablename__ = "flow"
    id: UUID = Field(
        default_factory=uuid4, index=True, primary_key=True
    )  # Column(Uuid, default=uuid4, index=True, primary_key=True)
    function_id: UUID | None = Field(
        foreign_key="function.id"
    )  # Column(Uuid, db.ForeignKey("function.id"))
    function_args: dict | list = Field(default_factory=dict, sa_column=Column(JSON))
    pull_function_id: UUID | None = Field(nullable=False)  # Column(Uuid)
    commit_function_id: UUID | None = Field(nullable=False)  # Column(Uuid)
    email: str | None = Field(default=None)  # Column(String)
    description: str | None = Field(default=None)  # Column(String)
    timer: int | None = Field(default=None)  # Column(Integer)
    timer_job_id: UUID | None = Field(default=None)  # Column(String)
    policy: int = Field(nullable=False)  # Column(Integer)
    # Aware UTC, compared against DataVersion.created_at by the ANY/ALL rerun
    # gate. sa_type is spelled out rather than inferred because only some
    # sqlmodel releases read a bare `datetime` as timezone-aware.
    last_executed: datetime | None = Field(default=None, sa_type=UTCDateTime)
    user_endpoint: UUID | None = Field(default=None)  # Column(String)
    arg_hash: str | None = Field(default=None)  # Column(String)
    derived_from: list["Data"] = Relationship(link_model=FlowDerivation)
    contributed_to: list["Data"] = Relationship(link_model=FlowContribution)
    function: Optional["Function"] = Relationship()
    # delete-orphan because DELETE /data/{id}/flows does session.delete(flow),
    # and FlowFailover.flow_id is a non-nullable FK that would otherwise block it
    # -- the same way Provenance does, which that route has to delete by hand.
    failovers: list["FlowFailover"] = Relationship(
        back_populates="flow",
        sa_relationship_kwargs={
            "cascade": "all, delete-orphan",
            "order_by": "FlowFailover.priority",
        },
    )

    def _select_run_target(self) -> "RunTarget":
        """Which endpoint and functions this run should go to.

        The primary unless it reports offline and an enabled failover does not.
        Checked fresh per run (behind the status cache) rather than stored, so
        nothing here is written back to the flow.

        A flow with no enabled failover rows short-circuits before any status
        call: with nowhere to go, knowing the endpoint is down would change
        nothing, and this runs inline in the notify webhook.
        """
        primary = RunTarget(
            endpoint=self.user_endpoint,
            function_id=self.function_id,
            pull_function_id=self.pull_function_id,
            commit_function_id=self.commit_function_id,
        )

        candidates = [f for f in self.failovers if f.enabled]
        if not Config.FAILOVER_ENABLED or not candidates:
            return primary

        if endpoint_is_up(self.user_endpoint) is not False:
            return primary

        for failover in candidates:
            if endpoint_is_up(failover.endpoint) is False:
                continue

            logger.warning(
                "flow %s: endpoint %s is offline, failing over to %s (priority %s)",
                self.id,
                self.user_endpoint,
                failover.endpoint,
                failover.priority,
            )
            return RunTarget(
                endpoint=failover.endpoint,
                # An unset function on the failover row means "the same one":
                # only an endpoint on a different Python minor version needs its
                # functions re-registered under new uuids.
                function_id=failover.function_id or self.function_id,
                pull_function_id=failover.pull_function_id or self.pull_function_id,
                commit_function_id=(
                    failover.commit_function_id or self.commit_function_id
                ),
                is_failover=True,
                reason=f"endpoint {self.user_endpoint} is offline",
            )

        # Submitting to a dead endpoint is still better than dropping the
        # trigger: the run fails visibly in Globus and `aero run --force`
        # recovers it, whereas a skipped run leaves nothing to find.
        logger.warning(
            "flow %s: endpoint %s and all %d failover(s) report offline -- "
            "submitting to the primary anyway",
            self.id,
            self.user_endpoint,
            len(candidates),
        )
        return RunTarget(
            endpoint=primary.endpoint,
            function_id=primary.function_id,
            pull_function_id=primary.pull_function_id,
            commit_function_id=primary.commit_function_id,
            reason="the primary and every failover report offline",
        )

    def _start_timer_flow(self, session: Session):
        self.timer_job_id = GLOBUS_CLIENT.set_timer(
            self.timer,
            self.id,
            self.email,
            FlowEnum.USER_FLOW,
            user_function=self.function_id,
            pull_function_uuid=self.pull_function_id,
            commit_function_uuid=self.commit_function_id,
            function_args=self.function_args,
            user_endpoint=self.user_endpoint,
        )

        return self.timer_job_id

    # TODO: remove all the execution-related parts from data and put in here
    def _start_ingestion_flow(self, session: Session, flush=False) -> UUID:
        if not flush and self.timer_job_id is not None:
            raise ServiceError(
                FLOW_TIMER_ERROR, "Flow already has a flow timer assigned"
            )

        self.timer_job_id = GLOBUS_CLIENT.set_timer(
            self.timer,
            self.id,
            self.email,
            FlowEnum.VERIFY_AND_MODIFY,
            user_function=self.function_id,
            pull_function_uuid=self.pull_function_id,
            commit_function_uuid=self.commit_function_id,
            function_args=self.function_args,
            user_endpoint=self.user_endpoint,
        )

        return self.timer_job_id

    def _run_ingestion_flow(
        self,
        session: Session,
        source_url: str | None = None,
        source_key: str | None = None,
        dedup: bool = True,
    ) -> None:
        """Run the ingestion flow once, immediately (event-driven; no timer).

        Invoked by the ``POST /data/{id}/notify`` webhook when the upstream
        source (e.g. an S3 object) reports a change. The flow re-pulls the
        source and its commit function records a new version.

        ``source_url`` overrides the registered source url for this run only
        (e.g. a MinIO presigned URL from the notify); it is never persisted.
        ``source_key`` and ``dedup`` ride along the same way, reaching
        ``add_new_version`` via the worker's ``/prov/new`` post so that a typed
        source dedups per url on the copy path too.
        """
        target = self._select_run_target()

        GLOBUS_CLIENT.run_ingestion_flow(
            self.id,
            self.email,
            user_function=target.function_id,
            pull_function_uuid=target.pull_function_id,
            commit_function_uuid=target.commit_function_id,
            function_args=self.function_args,
            user_endpoint=target.endpoint,
            source_url=source_url,
            source_key=source_key,
            dedup=dedup,
        )
        self.last_executed = utcnow()
        session.add(self)
        session.commit()
        session.refresh(self)

    def _has_new_input(self, require_all: bool) -> bool:
        """Whether the inputs have moved on since this flow last ran.

        ``require_all`` distinguishes ALL_INPUT from ANY_INPUT: every input must
        be newer, rather than at least one.

        Regardless of policy, **every** input must have a version. The run
        resolves each one's latest through ``GET /data/{id}/latest``, which 404s
        for a source that has been registered but never notified, failing the
        whole run — and an analysis cannot be handed a file for an input that has
        no data yet in any case.
        """
        if not self.derived_from:
            return False

        versions = [s.last_version() for s in self.derived_from]
        created = [
            v.created_at for v in versions if v is not None and v.created_at is not None
        ]
        if len(created) != len(versions):
            return False

        # Never run before: every input has data, so all of it is new. This has
        # to stay inside the has-a-version check above -- short-circuiting the
        # whole method on `last_executed is None` makes the first trigger run
        # unconditionally, which silently turns ALL_INPUT into ANY_INPUT.
        if self.last_executed is None:
            return True

        fresh = [c > self.last_executed for c in created]
        return all(fresh) if require_all else any(fresh)

    def _input_run_decision(self, force: bool) -> tuple[bool, str | None]:
        """Whether an ANY/ALL_INPUT flow should submit a run now, and why not if not.

        Pure over the inputs' latest versions, ``last_executed`` and ``force``, so
        asking ahead of ``_run_flow`` returns the same answer that ``_run_flow``
        will act on. ``POST /flow/{id}/run`` relies on that to report whether it
        submitted.

        ``force`` skips the "has an input moved on since we last ran" test and
        nothing else. Every input must still *have* a version: the run resolves
        each one through ``GET /data/{id}/latest``, which 404s for a source that
        was registered and never notified, and an analysis cannot be handed a file
        that does not exist. A flow with no inputs at all never runs this way
        either -- there is nothing for the pull function to fetch.
        """
        if not self.derived_from:
            return False, "the flow has no inputs"

        latest = {str(s.id): s.last_version() for s in self.derived_from}
        missing = sorted(
            k for k, v in latest.items() if v is None or v.created_at is None
        )
        if missing:
            return False, f"inputs with no version yet: {', '.join(missing)}"

        if force or self._has_new_input(
            require_all=self.policy == TriggerEnum.ALL_INPUT
        ):
            return True, None

        ages = {k: v.created_at for k, v in latest.items()}
        return False, (
            f"no input newer than last_executed={self.last_executed} "
            f"(input latest versions: {ages})"
        )

    def _run_flow(
        self,
        session: Session,
        trigger_url: str | None = None,
        signed_url: str | None = None,
        source_data_id: UUID | None = None,
        at_registration: bool = False,
        force: bool = False,
        target: "RunTarget | None" = None,
    ) -> int:
        # try:
        function_args = self.function_args
        # except json.JSONDecodeError as e:
        #     print(f"WARNING: Function args cannot be loaded: {e}")

        if self.policy == TriggerEnum.INGESTION:
            self._start_ingestion_flow(session=session)
        elif self.policy == TriggerEnum.INGESTION_EVENT:
            # Event-driven ingestion: no timer and no run at registration. The
            # flow runs when POST /data/{id}/notify calls _run_ingestion_flow().
            return self.policy
        elif self.policy == TriggerEnum.TIMER:
            self._start_timer_flow(session=session)
            self.last_executed = utcnow()
        elif self.policy in (TriggerEnum.ANY_INPUT, TriggerEnum.ALL_INPUT):
            logger.debug(
                "flow %s: policy=%s last_executed=%s at_registration=%s inputs=%s",
                self.id,
                self.policy,
                self.last_executed,
                at_registration,
                [str(s.id) for s in self.derived_from],
            )

            if at_registration and any(s.no_copy for s in self.derived_from):
                # Registration-time run against a no-copy input: there is no notify
                # in flight, so no signed url exists and a private object can't be
                # read. These flows are event-driven — let the first notify run it.
                #
                # This has to be an explicit flag, not `last_executed is None`:
                # skipping the run leaves last_executed None, so inferring it would
                # match every later notify too and the flow would never run at all.
                logger.info(
                    "flow %s: skipping the registration-time run, a no-copy input "
                    "has no signed url outside a notify",
                    self.id,
                )
                return self.policy

            submit, reason = self._input_run_decision(force=force)
            if submit:
                # Passed in by POST /flow/{id}/run, which resolves it first so
                # the response can name where the run went. Selecting again here
                # would re-log the failover and, with caching off, re-ask Globus.
                target = target or self._select_run_target()
                logger.info(
                    "flow %s: inputs ready, submitting run to %s%s (force=%s)",
                    self.id,
                    target.endpoint,
                    " [failover]" if target.is_failover else "",
                    force,
                )
                GLOBUS_CLIENT.run_flow(
                    id=self.id,
                    endpoint_uuid=target.endpoint,
                    function_uuid=target.function_id,
                    pull_function_uuid=target.pull_function_id,
                    commit_function_uuid=target.commit_function_id,
                    tasks=function_args,
                    email=self.email,
                    trigger_url=trigger_url,
                    signed_url=signed_url,
                    source_data_id=source_data_id,
                    primary_endpoint=self.user_endpoint,
                )
                # Stamped on submission, not on success. A run that fails still
                # moves last_executed forward, so every version that existed
                # before it is now "old" and will not trigger the flow again --
                # which is what the manual run endpoint's force is for.
                self.last_executed = utcnow()
            else:
                # The usual cause of "the analysis didn't run".
                logger.info("flow %s: not running -- %s", self.id, reason)

        else:
            return self.policy

        session.add(self)
        session.commit()
        session.refresh(self)

        return self.policy


def create_flow(
    session: Session,
    derived_from: list["Data"],
    contributed_to: list["Data"],
    endpoint: UUID | None = None,
    arg_hash: str | None = None,
    function_id: UUID | None = None,
    pull_function_id: UUID | None = None,
    commit_function_id: UUID | None = None,
    description: str = "",
    function_args: dict | list = {"aero": {}},
    timer: int | None = None,
    policy: TriggerEnum = TriggerEnum.NONE,
    email: str | None = None,
    failovers: list["FlowFailover"] | None = None,
):
    if policy == TriggerEnum.INGESTION and timer is None:
        timer = 86400

    last_executed = None
    id = uuid4()

    if isinstance(function_args, list):
        task_list = []
        task_invocation: dict = {}

        for fargs in function_args:
            fargs["aero"]["flow_id"] = str(id)

            task_invocation["kwargs"] = fargs
            task_invocation["function"] = str(function_id)
            task_invocation["endpoint"] = str(endpoint)

            task_list.append(task_invocation)

    else:
        task_list = {}
        function_args["aero"]["flow_id"] = str(id)
        task_list["kwargs"] = function_args
        task_list["function"] = str(function_id)
        task_list["endpoint"] = str(endpoint)

    function_args = task_list

    f = Flow(
        id=id,
        function_id=function_id,
        pull_function_id=pull_function_id,
        commit_function_id=commit_function_id,
        derived_from=derived_from,
        contributed_to=contributed_to,
        description=description,
        function_args=function_args,
        timer=timer,
        policy=policy,
        last_executed=last_executed,
        user_endpoint=endpoint,
        arg_hash=arg_hash,
        email=email,
        # Attached before the commit below, so the registration-time run already
        # has somewhere to fail over to.
        failovers=failovers or [],
    )

    session.add(f)
    session.commit()
    session.refresh(f)

    f._run_flow(session=session, at_registration=True)

    return f
