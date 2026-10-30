"""Managing a flow's failover endpoints over the API."""

import pytest

from uuid import uuid4

import aero.models.flows

from aero.globus import health
from aero.models.flows import TriggerEnum


@pytest.fixture(name="statuses")
def statuses_fixture(monkeypatch):
    """Drive the health check from a {endpoint: status string} map."""
    table: dict[str, str] = {}

    def fake(endpoint_uuid):
        return {"status": table.get(str(endpoint_uuid), "nothing-known")}

    monkeypatch.setattr(health, "_query_status", lambda e: _verdict(fake(e)))
    health.clear_cache()
    return table


def _verdict(response):
    status = response.get("status")
    return True if status == "online" else (False if status == "offline" else None)


@pytest.fixture(name="analysis")
def analysis_fixture(session, data):
    """An ANY_INPUT flow -- the only policy POST /flow/{id}/run accepts."""
    return aero.models.flows.create_flow(
        session=session,
        derived_from=[data],
        contributed_to=[],
        endpoint=uuid4(),
        function_id=aero.models.function.create_function(
            session=session, uuid=uuid4()
        ).id,
        pull_function_id=uuid4(),
        commit_function_id=uuid4(),
        policy=TriggerEnum.ANY_INPUT,
    )


def _add(client, flow_id, endpoint, **body):
    return client.post(
        f"/flow/{flow_id}/failover", json={"endpoint": str(endpoint), **body}
    )


def test_add_and_list(client, flow):
    backup = uuid4()

    r = _add(client, flow.id, backup, priority=2)
    assert r.status_code == 200, r.content
    assert r.json()["endpoint"] == str(backup)
    assert r.json()["priority"] == 2
    assert r.json()["enabled"] is True
    # Unset function uuids mean "reuse the flow's own".
    assert r.json()["function_id"] is None

    listed = client.get(f"/flow/{flow.id}/failover").json()
    assert [x["endpoint"] for x in listed] == [str(backup)]


def test_list_is_in_priority_order(client, flow):
    second, first = uuid4(), uuid4()
    _add(client, flow.id, second, priority=9)
    _add(client, flow.id, first, priority=1)

    listed = client.get(f"/flow/{flow.id}/failover").json()
    assert [x["endpoint"] for x in listed] == [str(first), str(second)]


def test_explicit_functions_are_kept(client, flow):
    fn, pull, commit = uuid4(), uuid4(), uuid4()

    r = _add(
        client,
        flow.id,
        uuid4(),
        function_uuid=str(fn),
        pull_function_uuid=str(pull),
        commit_function_uuid=str(commit),
    )

    assert r.json()["function_id"] == str(fn)
    assert r.json()["pull_function_id"] == str(pull)
    assert r.json()["commit_function_id"] == str(commit)


def test_duplicate_priority_is_rejected(client, flow):
    _add(client, flow.id, uuid4(), priority=0)

    r = _add(client, flow.id, uuid4(), priority=0)

    assert r.status_code == 409
    assert "priority" in r.json()["detail"]


def test_a_failover_to_the_primary_is_rejected(client, flow):
    r = _add(client, flow.id, flow.user_endpoint)

    assert r.status_code == 400
    assert "no-op" in r.json()["detail"]


def test_delete(client, flow):
    created = _add(client, flow.id, uuid4()).json()

    r = client.delete(f"/flow/{flow.id}/failover/{created['id']}")

    assert r.status_code == 200
    assert r.json()["id"] == created["id"]
    assert client.get(f"/flow/{flow.id}/failover").json() == []


def test_delete_an_unknown_one_404s(client, flow):
    r = client.delete(f"/flow/{flow.id}/failover/{uuid4()}")

    assert r.status_code == 404


def test_delete_will_not_reach_across_flows(client, session, flow, data):
    """A failover id is only valid under the flow that owns it."""
    other = aero.models.flows.create_flow(
        session=session,
        derived_from=[data],
        contributed_to=[],
        endpoint=uuid4(),
        function_id=aero.models.function.create_function(
            session=session, uuid=uuid4()
        ).id,
        pull_function_id=uuid4(),
        commit_function_id=uuid4(),
    )
    created = _add(client, flow.id, uuid4()).json()

    r = client.delete(f"/flow/{other.id}/failover/{created['id']}")

    assert r.status_code == 404
    assert client.get(f"/flow/{flow.id}/failover").json() != []


def test_routes_404_for_an_unknown_flow(client):
    unknown = uuid4()

    assert client.get(f"/flow/{unknown}/failover").status_code == 404
    assert _add(client, unknown, uuid4()).status_code == 404
    assert client.get(f"/flow/{unknown}/endpoint-status").status_code == 404


def test_endpoint_status_reports_every_candidate(client, flow, statuses):
    backup = uuid4()
    _add(client, flow.id, backup, priority=3)
    statuses[str(flow.user_endpoint)] = "offline"
    statuses[str(backup)] = "online"

    body = client.get(f"/flow/{flow.id}/endpoint-status").json()

    assert body == [
        {
            "endpoint": str(flow.user_endpoint),
            "role": "primary",
            "priority": None,
            "enabled": True,
            "status": "offline",
        },
        {
            "endpoint": str(backup),
            "role": "failover",
            "priority": 3,
            "enabled": True,
            "status": "online",
        },
    ]


def test_endpoint_status_says_unknown_when_it_cannot_tell(client, flow, statuses):
    """The symptom of a credential with no role on the endpoint.

    Worth surfacing: an unanswerable check never triggers failover, so without
    this the configuration looks fine and simply never fires.
    """
    body = client.get(f"/flow/{flow.id}/endpoint-status").json()

    assert body[0]["status"] == "unknown"


def test_register_accepts_failover(client, data):
    r = client.post(
        "/flow/register",
        json={
            "gc_endpoint": str(uuid4()),
            "function_uuid": str(uuid4()),
            "pull_function_uuid": str(uuid4()),
            "commit_function_uuid": str(uuid4()),
            "input_data": {"in": {"id": str(data.id), "version": None}},
            "output_data": {},
            "rule": int(TriggerEnum.ANY_INPUT),
            "failover": [{"endpoint": str(uuid4()), "priority": 1}],
        },
    )
    assert r.status_code == 200, r.content

    listed = client.get(f"/flow/{r.json()['id']}/failover").json()
    assert len(listed) == 1
    assert listed[0]["priority"] == 1


def test_register_rejects_duplicate_priorities(client, data):
    r = client.post(
        "/flow/register",
        json={
            "gc_endpoint": str(uuid4()),
            "function_uuid": str(uuid4()),
            "pull_function_uuid": str(uuid4()),
            "commit_function_uuid": str(uuid4()),
            "input_data": {"in": {"id": str(data.id), "version": None}},
            "output_data": {},
            "rule": int(TriggerEnum.ANY_INPUT),
            "failover": [
                {"endpoint": str(uuid4()), "priority": 0},
                {"endpoint": str(uuid4()), "priority": 0},
            ],
        },
    )

    assert r.status_code == 409
    assert "Duplicate" in r.json()["detail"]


def test_deleting_the_data_flows_clears_failovers(client, session, flow, data):
    """Regression: FlowFailover.flow_id is a non-nullable FK.

    Without delete-orphan on the relationship, DELETE /data/{id}/flows raised an
    integrity error on any flow that had a failover configured.
    """
    _add(client, flow.id, uuid4())

    r = client.delete(f"/data/{data.id}/flows")

    assert r.status_code == 200, r.content
    assert client.get(f"/flow/{flow.id}").status_code == 404


def test_run_reports_the_endpoint_it_used(client, analysis, monkeypatch):
    backup = uuid4()
    _add(client, analysis.id, backup)

    def fake(endpoint_uuid, use_cache=True):
        return False if str(endpoint_uuid) == str(analysis.user_endpoint) else True

    monkeypatch.setattr(aero.models.flows, "endpoint_is_up", fake)

    r = client.post(f"/flow/{analysis.id}/run", json={"force": True})

    assert r.status_code == 200, r.content
    assert r.json()["submitted"] is True
    assert r.json()["endpoint"] == str(backup)
    assert r.json()["failover"] is True


def test_run_reports_the_primary_when_healthy(client, analysis):
    r = client.post(f"/flow/{analysis.id}/run", json={"force": True})

    assert r.json()["endpoint"] == str(analysis.user_endpoint)
    assert r.json()["failover"] is False


def test_a_skipped_run_names_no_endpoint(client, session, analysis, noversion_data):
    """No submission, so nothing was selected and no status was checked."""
    analysis.derived_from.append(noversion_data)
    session.add(analysis)
    session.commit()

    r = client.post(f"/flow/{analysis.id}/run", json={"force": False})

    assert r.json()["submitted"] is False
    assert r.json()["endpoint"] is None
    assert r.json()["failover"] is False
