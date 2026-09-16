import copy
import datetime
import json
import logging
import os
from functools import lru_cache
from typing import TypeAlias
from uuid import UUID

from globus_sdk import AuthClient
from globus_sdk import ClientApp
from globus_sdk import FlowTimer
from globus_sdk import RecurringTimerSchedule
from globus_sdk import SearchClient
from globus_sdk import SpecificFlowClient
from globus_sdk import TimersClient
from globus_sdk.scopes import TimersScopes
from globus_sdk.services.search.errors import SearchAPIError

from aero.config import Config

from aero.globus.utils import FlowEnum
from aero.globus.utils import FLOW_IDS

JSON: TypeAlias = dict[str, "JSON"] | list["JSON"] | str | int | float | bool | None

logger = logging.getLogger(__name__)


class GlobusClient:
    app: ClientApp
    auth_client: AuthClient
    timer_client: TimersClient
    search_client: SearchClient
    search_index: str

    def __init__(self, search_index=None):
        self.app = ClientApp(
            "aero",
            client_id=Config.PORTAL_CLIENT_ID,
            client_secret=Config.PORTAL_CLIENT_SECRET,
        )
        self.specific_flow_clients = {}

        for flow_id in FLOW_IDS.values():
            self.specific_flow_clients[flow_id] = SpecificFlowClient(
                flow_id=flow_id, app=self.app
            )

        self.auth_client = AuthClient(app=self.app)
        self.timer_client = TimersClient(app=self.app, app_scopes=self._timer_scopes())
        self.search_client = SearchClient(app=self.app)
        self.search_client.add_app_scope(self.search_client.scopes.all)
        self.search_index = search_index

    def _create_search_idx(self):
        if self.search_index is None:
            r = self.search_client.create_index(
                "AERO data",
                "Searchable index for all AERO data",
            )
            self.search_index = r["id"]
            print(f"Created new search index: {self.search_index}")

    def _timer_scopes(self):
        flow_user_scopes = [
            sfc.scopes.user for sfc in self.specific_flow_clients.values()
        ]
        timer_scope = TimersScopes.timer.with_dependencies(flow_user_scopes)

        return [timer_scope]

    def add_search_entry(self, entry: dict) -> str | None:
        """Index a version in Globus Search. Best-effort — never raises.

        Returns the ingest response text, or None if indexing was skipped or
        failed. Failures are logged rather than returned, so a caller cannot
        mistake an error payload for a result.
        """
        if not Config.SEARCH_ENABLED:
            logger.debug("Globus Search disabled, not indexing")
            return None

        try:
            response = self.search_client.ingest(self.search_index, entry)
            return response.text
        except SearchAPIError as e:
            logger.warning(
                "Globus Search ingest failed (%s), continuing without indexing: %s",
                e.http_status,
                e.message,
            )
            return None
        except Exception as e:  # auth failures etc. -- still must not break ingest
            logger.warning(
                "Globus Search ingest failed (%s), continuing without indexing: %s",
                type(e).__name__,
                e,
            )
            return None

    @lru_cache
    def get_user_uuid(self, usernames: str):
        return self.auth_client.get_identities(usernames=usernames)["identities"][0][
            "id"
        ]

    def run_flow(
        self,
        id,
        endpoint_uuid: str,
        function_uuid: str,
        pull_function_uuid: str,
        commit_function_uuid: str,
        tasks: JSON,
        email: str | None,
        trigger_url: str | None = None,
        signed_url: str | None = None,
        source_data_id=None,
    ):
        """Run a user (analysis) flow.

        ``id`` is the AERO flow's uuid, and goes into the run label so that runs
        of different analyses can be told apart in the Globus run list.

        ``trigger_url``/``signed_url``, when given, are injected onto the one
        ``input_data`` entry matching ``source_data_id`` — the no-copy source whose
        change triggered this run. They go into a deep copy of the tasks so the
        transient signed url is never written back to ``Flow.function_args``.
        """
        flow_id = FLOW_IDS[FlowEnum.USER_FLOW]

        monitors = []

        if isinstance(tasks, dict):
            tasks = [tasks]

        if trigger_url and source_data_id is not None:
            tasks = copy.deepcopy(tasks)
            for task in tasks:
                input_data = task.get("kwargs", {}).get("aero", {}).get(
                    "input_data", {}
                )
                for entry in input_data.values():
                    if entry.get("id") == str(source_data_id):
                        entry["trigger_url"] = trigger_url
                        if signed_url:
                            entry["signed_url"] = signed_url

        if email is not None:
            user_uuid = self.get_user_uuid(usernames=email)
            monitors.append(user_uuid)

        run_input = {
            "endpoint": endpoint_uuid,
            "version_function": pull_function_uuid,
            "commit_function": commit_function_uuid,
            "function": function_uuid,
            "tasks": tasks,
        }
        response = self.specific_flow_clients[flow_id].run_flow(
            body=run_input,
            label=f"AERO Analysis flow {str(id)[:8]}",
            run_managers=monitors,
        )
        assert response.http_status == 201

    def set_timer(
        self,
        interval_in_sec: int,
        id: int,
        email: str,
        flow_type: FlowEnum,
        user_function: str,
        pull_function_uuid: str,
        commit_function_uuid: str,
        function_args: str,
        user_endpoint: str,
        **kwargs,
    ) -> UUID:
        """Set a Globus Timer for daily retrieval of updated tables from sources.

        Arguments:
            func_uuid: The Globus Compute registered function UUID.
        """
        flow_id = FLOW_IDS[flow_type]

        if email is None:
            email = ""

        kwargs = function_args["kwargs"]
        if flow_type == FlowEnum.VERIFY_AND_MODIFY:
            run_input = {
                "osprey-worker-endpoint": str(user_endpoint),
                "download-function": pull_function_uuid,
                "database-commit-function": commit_function_uuid,
                "user-wrapper-function": user_function,
                "kwargs": json.dumps(kwargs),
                "author-email": email,
                "_private_password": os.environ.get("DSAAS_EMAIL_PASSWORD"),
            }

            name = f"AERO-ingestion-{str(id)[:8]}"

        else:
            run_input = {
                "endpoint": user_endpoint,
                "version_function": pull_function_uuid,
                "commit_function": commit_function_uuid,
                "function": user_function,
                "tasks": json.dumps(kwargs),
            }

            name = f"AERO-user-flow-{id}"

        flow_timer = FlowTimer(
            flow_id=flow_id,
            name=name,
            schedule=RecurringTimerSchedule(
                interval_seconds=interval_in_sec,
                start=datetime.datetime.now(),
            ),
            body={"body": run_input},
        )

        response = self.timer_client.create_timer(flow_timer)
        assert response.http_status == 201
        job_id = UUID(response["timer"]["job_id"])
        return job_id

    def run_ingestion_flow(
        self,
        id,
        email: str | None,
        user_function: str,
        pull_function_uuid: str,
        commit_function_uuid: str,
        function_args,
        user_endpoint: str,
        source_url: str | None = None,
        source_key: str | None = None,
        dedup: bool = True,
        **kwargs,
    ) -> None:
        """Run the ingestion (VERIFY_AND_MODIFY) flow once, immediately.

        Uses the same run input as the timer-based ingestion path in
        ``set_timer``, but submits it directly to the flow instead of scheduling
        it on a Globus Timer. Used for event-driven (INGESTION_EVENT) sources.

        ``source_url``, when given, overrides the source url for this run only
        (e.g. a MinIO presigned URL). ``source_key`` and ``dedup`` travel the same
        way and come back to the server on the worker's ``/prov/new`` post, where
        they drive per-url change detection. All three are injected into a deep
        copy of the run kwargs so none is persisted onto ``Flow.function_args``.
        """
        flow_id = FLOW_IDS[FlowEnum.VERIFY_AND_MODIFY]

        if email is None:
            email = ""

        kwargs = copy.deepcopy(function_args["kwargs"])
        if source_url:
            kwargs.setdefault("aero", {})["source_url"] = source_url
        if source_key:
            kwargs.setdefault("aero", {})["source_key"] = source_key
        if not dedup:
            kwargs.setdefault("aero", {})["dedup"] = False
        run_input = {
            "osprey-worker-endpoint": str(user_endpoint),
            "download-function": pull_function_uuid,
            "database-commit-function": commit_function_uuid,
            "user-wrapper-function": user_function,
            "kwargs": json.dumps(kwargs),
            "author-email": email,
            "_private_password": os.environ.get("DSAAS_EMAIL_PASSWORD"),
        }

        response = self.specific_flow_clients[flow_id].run_flow(
            body=run_input,
            label=f"AERO Demo | Ingestion flow {str(id)[:8]}",
            run_managers=[],
        )
        assert response.http_status == 201

    def delete_job(self, job_id: str):
        response = self.timer_client.delete_job(job_id=job_id)
        assert response.http_status == 200, response.http_reason
