import pytest

from uuid import uuid4

import aero.models.flows

from aero.config import Config
from aero.models.failover import FlowFailover
from aero.models.flows import TriggerEnum


PRIMARY = uuid4()


@pytest.fixture(name="analysis")
def analysis_fixture(globus_mock, session, data):
    """An ANY_INPUT flow on a known primary endpoint, with no failovers yet."""
    func = aero.models.function.create_function(session=session, uuid=uuid4())
    p_func = aero.models.function.create_function(session=session, uuid=uuid4())
    c_func = aero.models.function.create_function(session=session, uuid=uuid4())

    return aero.models.flows.create_flow(
        session=session,
        derived_from=[data],
        contributed_to=[],
        endpoint=PRIMARY,
        function_id=func.id,
        pull_function_id=p_func.id,
        commit_function_id=c_func.id,
        policy=TriggerEnum.ANY_INPUT,
    )


@pytest.fixture(name="statuses")
def statuses_fixture(monkeypatch):
    """Drive endpoint_is_up from a {endpoint: verdict} map and count lookups."""
    table: dict[str, bool | None] = {}
    asked: list[str] = []

    def fake(endpoint_uuid, use_cache=True):
        asked.append(str(endpoint_uuid))
        return table.get(str(endpoint_uuid))

    monkeypatch.setattr(aero.models.flows, "endpoint_is_up", fake)
    return table, asked


def _add_failover(session, flow, endpoint, priority=0, **kwargs):
    row = FlowFailover(flow_id=flow.id, endpoint=endpoint, priority=priority, **kwargs)
    session.add(row)
    session.commit()
    session.refresh(flow)
    return row


def test_no_failovers_means_no_status_check(analysis, statuses):
    """The check is skipped entirely when there is nowhere to go.

    It runs inline in the notify webhook, so a flow that cannot fail over must
    not pay a Globus round-trip to learn something it could not act on.
    """
    table, asked = statuses
    table[str(PRIMARY)] = False

    target = analysis._select_run_target()

    assert asked == []
    assert target.endpoint == PRIMARY
    assert target.is_failover is False


def test_primary_online_wins(session, analysis, statuses):
    table, asked = statuses
    backup = uuid4()
    _add_failover(session, analysis, backup)
    table[str(PRIMARY)] = True
    table[str(backup)] = True

    target = analysis._select_run_target()

    assert target.endpoint == PRIMARY
    assert target.is_failover is False
    assert str(backup) not in asked, "no reason to check a failover we won't use"


def test_unknown_primary_is_not_failed_over(session, analysis, statuses):
    """An unanswerable check must not reroute the run.

    A 403 or a timeout says nothing about the endpoint, and treating it as down
    would move every run the moment Globus Auth hiccuped.
    """
    table, _ = statuses
    _add_failover(session, analysis, uuid4())
    table[str(PRIMARY)] = None

    target = analysis._select_run_target()

    assert target.endpoint == PRIMARY
    assert target.is_failover is False


def test_offline_primary_falls_over(session, analysis, statuses):
    table, _ = statuses
    backup = uuid4()
    _add_failover(session, analysis, backup)
    table[str(PRIMARY)] = False
    table[str(backup)] = True

    target = analysis._select_run_target()

    assert target.endpoint == backup
    assert target.is_failover is True
    assert target.reason is not None


def test_functions_inherit_when_unset(session, analysis, statuses):
    table, _ = statuses
    backup = uuid4()
    _add_failover(session, analysis, backup)
    table[str(PRIMARY)] = False
    table[str(backup)] = True

    target = analysis._select_run_target()

    assert target.function_id == analysis.function_id
    assert target.pull_function_id == analysis.pull_function_id
    assert target.commit_function_id == analysis.commit_function_id


def test_functions_override_when_set(session, analysis, statuses):
    """A failover endpoint on a different Python needs its own function uuids."""
    table, _ = statuses
    backup, alt_fn, alt_pull = uuid4(), uuid4(), uuid4()
    _add_failover(
        session,
        analysis,
        backup,
        function_id=alt_fn,
        pull_function_id=alt_pull,
    )
    table[str(PRIMARY)] = False
    table[str(backup)] = True

    target = analysis._select_run_target()

    assert target.function_id == alt_fn
    assert target.pull_function_id == alt_pull
    # Left unset on the row, so it still comes from the flow.
    assert target.commit_function_id == analysis.commit_function_id


def test_chain_is_tried_in_priority_order(session, analysis, statuses):
    table, _ = statuses
    first, second = uuid4(), uuid4()
    _add_failover(session, analysis, second, priority=5)
    _add_failover(session, analysis, first, priority=1)
    table[str(PRIMARY)] = False
    table[str(first)] = False
    table[str(second)] = True

    target = analysis._select_run_target()

    assert target.endpoint == second


def test_unknown_failover_is_used(session, analysis, statuses):
    """Only an explicit offline disqualifies a candidate, here as for the primary."""
    table, _ = statuses
    backup = uuid4()
    _add_failover(session, analysis, backup)
    table[str(PRIMARY)] = False
    table[str(backup)] = None

    assert analysis._select_run_target().endpoint == backup


def test_disabled_failovers_are_skipped(session, analysis, statuses):
    table, asked = statuses
    off, on = uuid4(), uuid4()
    _add_failover(session, analysis, off, priority=0, enabled=False)
    _add_failover(session, analysis, on, priority=1)
    table[str(PRIMARY)] = False
    table[str(on)] = True

    target = analysis._select_run_target()

    assert target.endpoint == on
    assert str(off) not in asked


def test_only_disabled_failovers_means_no_check(analysis, session, statuses):
    table, asked = statuses
    _add_failover(session, analysis, uuid4(), enabled=False)
    table[str(PRIMARY)] = False

    assert analysis._select_run_target().endpoint == PRIMARY
    assert asked == []


def test_all_offline_falls_back_to_primary(session, analysis, statuses):
    """Submitting to a dead endpoint beats dropping the trigger.

    The run fails visibly in Globus and `aero run --force` recovers it, whereas
    a skipped run leaves nothing to find -- and last_executed has moved either
    way.
    """
    table, _ = statuses
    backup = uuid4()
    _add_failover(session, analysis, backup)
    table[str(PRIMARY)] = False
    table[str(backup)] = False

    target = analysis._select_run_target()

    assert target.endpoint == PRIMARY
    assert target.is_failover is False
    assert "offline" in target.reason


def test_kill_switch_skips_selection(session, analysis, statuses, monkeypatch):
    table, asked = statuses
    backup = uuid4()
    _add_failover(session, analysis, backup)
    table[str(PRIMARY)] = False
    table[str(backup)] = True
    monkeypatch.setattr(Config, "FAILOVER_ENABLED", False)

    target = analysis._select_run_target()

    assert target.endpoint == PRIMARY
    assert asked == []


def test_run_flow_submits_to_the_failover_without_rehoming(
    session, analysis, version, statuses, monkeypatch
):
    """The run goes elsewhere; the flow keeps naming its primary."""
    table, _ = statuses
    backup = uuid4()
    _add_failover(session, analysis, backup)
    table[str(PRIMARY)] = False
    table[str(backup)] = True

    sent = {}
    monkeypatch.setattr(
        aero.models.flows.GLOBUS_CLIENT, "run_flow", lambda **kw: sent.update(kw)
    )

    analysis._run_flow(session=session, force=True)

    assert sent["endpoint_uuid"] == backup
    assert sent["primary_endpoint"] == PRIMARY

    session.refresh(analysis)
    assert analysis.user_endpoint == PRIMARY


def test_ingestion_flow_also_fails_over(session, data, statuses, monkeypatch):
    """INGESTION_EVENT runs from the notify webhook and gets the same treatment."""
    table, _ = statuses
    backup = uuid4()

    flow = aero.models.flows.create_flow(
        session=session,
        derived_from=[],
        contributed_to=[data],
        endpoint=PRIMARY,
        function_id=aero.models.function.create_function(
            session=session, uuid=uuid4()
        ).id,
        pull_function_id=uuid4(),
        commit_function_id=uuid4(),
        policy=TriggerEnum.INGESTION_EVENT,
    )
    _add_failover(session, flow, backup)
    table[str(PRIMARY)] = False
    table[str(backup)] = True

    sent = {}
    monkeypatch.setattr(
        aero.models.flows.GLOBUS_CLIENT,
        "run_ingestion_flow",
        lambda *a, **kw: sent.update(kw),
    )

    flow._run_ingestion_flow(session=session)

    assert sent["user_endpoint"] == backup


def test_deleting_a_flow_takes_its_failovers(session, analysis):
    """FlowFailover.flow_id is non-nullable, so the cascade has to clear them.

    Regression: without delete-orphan, `DELETE /data/{id}/flows` fails with an
    integrity error on any flow that has a failover configured.
    """
    from sqlmodel import select

    _add_failover(session, analysis, uuid4())
    assert session.exec(select(FlowFailover)).all()

    session.delete(analysis)
    session.commit()

    assert session.exec(select(FlowFailover)).all() == []
