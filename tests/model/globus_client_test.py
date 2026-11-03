"""Per-run url injection into an analysis flow's input_data.

The transient signed url must reach the run without ever being written back to
the stored ``Flow.function_args`` — that is what the deep copy is for.
"""

import json

import pytest

from aero import GLOBUS_CLIENT
from aero.globus.utils import FlowEnum
from aero.globus.utils import FLOW_IDS


SRC = "8908fc25-4f6d-46ea-814e-af4d2efcfbb5"
OTHER = "11111111-1111-1111-1111-111111111111"
FLOW_ID = "6a7afff7-2c5e-4a1e-9a4d-0f3c2b1d8e90"
TRIGGER = "http://minio:9000/traffic/a.xml.gz"
SIGNED = f"{TRIGGER}?X-Amz-Signature=abc"


@pytest.fixture(name="sent")
def sent_fixture(monkeypatch):
    """Capture the run_input that would go to the Globus flow service."""
    captured = {}

    class _Resp:
        http_status = 201

    class _FlowClient:
        def run_flow(self, body, label, run_managers):
            captured.update(body)
            captured["_label"] = label
            return _Resp()

    monkeypatch.setitem(
        GLOBUS_CLIENT.specific_flow_clients, FLOW_IDS[FlowEnum.USER_FLOW], _FlowClient()
    )
    return captured


@pytest.fixture(name="tasks")
def tasks_fixture():
    """A two-input analysis flow's persisted function_args."""
    return {
        "kwargs": {
            "aero": {
                "input_data": {
                    "report": {
                        "id": SRC,
                        "version": None,
                        "collection_url": "https://c/",
                    },
                    "other": {
                        "id": OTHER,
                        "version": None,
                        "collection_url": "https://c/",
                    },
                }
            }
        },
        "function": "f",
        "endpoint": "e",
    }


def _run(tasks, **extra):
    extra.setdefault("id", FLOW_ID)
    GLOBUS_CLIENT.run_flow(
        endpoint_uuid="e",
        function_uuid="f",
        pull_function_uuid="p",
        commit_function_uuid="c",
        tasks=tasks,
        email=None,
        **extra,
    )


def test_urls_land_only_on_the_input_that_changed(sent, tasks):
    _run(tasks, trigger_url=TRIGGER, signed_url=SIGNED, source_data_id=SRC)

    input_data = sent["tasks"][0]["kwargs"]["aero"]["input_data"]
    assert input_data["report"]["trigger_url"] == TRIGGER
    assert input_data["report"]["signed_url"] == SIGNED
    # a sibling input resolves however it normally would
    assert "trigger_url" not in input_data["other"]


def test_stored_function_args_are_not_mutated(sent, tasks):
    before = json.dumps(tasks, sort_keys=True)

    _run(tasks, trigger_url=TRIGGER, signed_url=SIGNED, source_data_id=SRC)

    # the signed url expires; persisting it would poison every later run
    assert json.dumps(tasks, sort_keys=True) == before


def test_no_injection_without_a_trigger(sent, tasks):
    """The copy path and /prov/new-driven reruns pass no urls at all."""
    _run(tasks)

    input_data = sent["tasks"][0]["kwargs"]["aero"]["input_data"]
    assert not any("trigger_url" in entry for entry in input_data.values())


def test_trigger_without_a_signature_still_injects(sent, tasks):
    """Unsigned is a valid state: public bucket, or presigning turned off."""
    _run(tasks, trigger_url=TRIGGER, source_data_id=SRC)

    report = sent["tasks"][0]["kwargs"]["aero"]["input_data"]["report"]
    assert report["trigger_url"] == TRIGGER
    assert "signed_url" not in report


def test_the_run_label_names_the_flow(sent, tasks):
    """Every analysis run used to carry the same constant label.

    With nothing but "AERO Demo | User flow" in the Globus run list, runs of
    different analyses were indistinguishable.
    """
    _run(tasks)

    assert sent["_label"] == f"AERO Analysis flow {FLOW_ID[:8]}"


def test_a_uuid_object_labels_the_same_as_its_string(sent, tasks):
    """Flow.id is a UUID, not a str, so the label must not depend on which."""
    import uuid

    _run(tasks, id=uuid.UUID(FLOW_ID))

    assert sent["_label"] == f"AERO Analysis flow {FLOW_ID[:8]}"


# --- failover: repointing a run at a different endpoint ---------------------

BACKUP = "22222222-2222-2222-2222-222222222222"


def _failover_run(tasks, **extra):
    """A run whose selected endpoint differs from the flow's registered one."""
    extra.setdefault("id", FLOW_ID)
    GLOBUS_CLIENT.run_flow(
        endpoint_uuid=BACKUP,
        function_uuid="f2",
        pull_function_uuid="p2",
        commit_function_uuid="c2",
        tasks=tasks,
        email=None,
        primary_endpoint="e",
        **extra,
    )


def test_failover_moves_the_task_dicts_too(sent, tasks):
    """Regression: a failover run used to execute on the dead endpoint anyway.

    The user flow reads the endpoint twice. `run_input["endpoint"]` drives
    getVersions and updateMetadata, but the execute state dispatches on
    `$.VersionResult.details.result[0]` — the task dicts, which get_versions
    returns unchanged and which carry their own "endpoint"/"function" baked in
    by create_flow. Rewriting only the run input moved the pull and the commit
    while leaving the analysis itself pointed at the endpoint that was down.
    """
    _failover_run(tasks)

    assert sent["endpoint"] == BACKUP
    assert sent["version_function"] == "p2"
    assert sent["commit_function"] == "c2"
    assert sent["tasks"][0]["endpoint"] == BACKUP
    assert sent["tasks"][0]["function"] == "f2"


def test_failover_does_not_persist_onto_function_args(sent, tasks):
    """The flow keeps naming its primary; only this run moves."""
    before = json.dumps(tasks, sort_keys=True)

    _failover_run(tasks)

    assert json.dumps(tasks, sort_keys=True) == before


def test_no_rewrite_when_the_primary_is_the_target(sent, tasks):
    """The ordinary path: selection returned the primary, nothing to repoint."""
    _run(tasks, primary_endpoint="e")

    assert sent["tasks"][0]["endpoint"] == "e"
    assert sent["tasks"][0]["function"] == "f"


def test_a_task_on_another_endpoint_is_left_alone(sent, tasks):
    """Only tasks pointed at the primary move.

    A task naming some other endpoint was put there deliberately, and this
    failover knows nothing about whether that one is healthy.
    """
    other_task = {"kwargs": {"aero": {}}, "function": "fx", "endpoint": "elsewhere"}

    _failover_run([tasks, other_task])

    assert sent["tasks"][0]["endpoint"] == BACKUP
    assert sent["tasks"][1]["endpoint"] == "elsewhere"
    assert sent["tasks"][1]["function"] == "fx"


def test_failover_and_url_injection_compose(sent, tasks):
    """A notify-triggered no-copy run that also has to fail over gets both."""
    _failover_run(tasks, trigger_url=TRIGGER, signed_url=SIGNED, source_data_id=SRC)

    assert sent["tasks"][0]["endpoint"] == BACKUP
    report = sent["tasks"][0]["kwargs"]["aero"]["input_data"]["report"]
    assert report["signed_url"] == SIGNED


def test_without_a_primary_nothing_is_repointed(sent, tasks):
    """`primary_endpoint` is what says "this is a failover".

    Callers that do not pass it — the ingestion path, and anything older — must
    keep the behavior they had.
    """
    GLOBUS_CLIENT.run_flow(
        id=FLOW_ID,
        endpoint_uuid=BACKUP,
        function_uuid="f2",
        pull_function_uuid="p2",
        commit_function_uuid="c2",
        tasks=tasks,
        email=None,
    )

    assert sent["endpoint"] == BACKUP
    assert sent["tasks"][0]["endpoint"] == "e"
