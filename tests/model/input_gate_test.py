"""When an ANY/ALL analysis is allowed to run.

Two conditions, and they are independent: every input must have a version at
all, and then the policy decides how many of them must be newer than the last
run.
"""

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from uuid import uuid4

import pytest

import aero.models.data
import aero.models.data_version
import aero.models.flows as fm


def _data(session, name):
    return aero.models.data.create_data(
        session=session,
        name=name,
        url=f"http://minio/{name}.csv",
        collection_url="https://globus.org/test",
        collection_uuid=uuid4(),
        description="",
    )


def _version(session, data, when, checksum="c"):
    v = aero.models.data_version.create_dataversion(
        session=session, version=len(data.versions) + 1, checksum=checksum,
        data_id=data.id,
    )
    v.created_at = when
    session.add(v)
    session.commit()
    session.refresh(data)
    return v


def _flow(session, inputs, policy):
    fn = lambda: aero.models.function.create_function(session=session, uuid=uuid4()).id
    import aero.models.function  # noqa: F401

    return fm.Flow(
        function_id=None,
        pull_function_id=uuid4(),
        commit_function_id=uuid4(),
        derived_from=list(inputs),
        policy=policy,
    )


@pytest.fixture(name="no_globus", autouse=True)
def no_globus_fixture(monkeypatch):
    monkeypatch.setattr(fm.GLOBUS_CLIENT, "run_flow", lambda **kw: None)


NOW = datetime(2026, 8, 14, 12, 0, 0, tzinfo=timezone.utc)
EARLIER = NOW - timedelta(hours=1)
LATER = NOW + timedelta(hours=1)


def test_all_waits_for_every_input_on_the_very_first_trigger(session):
    """The case that failed live: A updated, B never notified, ALL still ran.

    Short-circuiting on `last_executed is None` made the first trigger run
    unconditionally, so ALL_INPUT behaved as ANY_INPUT exactly once -- and the
    run then died in get_versions resolving B's nonexistent latest version.
    """
    a, b = _data(session, "a"), _data(session, "b")
    _version(session, a, NOW)                      # only A has data
    flow = _flow(session, [a, b], fm.TriggerEnum.ALL_INPUT)

    assert flow.last_executed is None
    assert flow._has_new_input(require_all=True) is False

    _version(session, b, NOW)                      # now B does too
    assert flow._has_new_input(require_all=True) is True


def test_any_also_needs_every_input_to_exist(session):
    """Not a policy question: the run resolves every input's latest version.

    /data/{id}/latest 404s for a source with none, failing the whole task, and
    the function has no file to receive for that parameter regardless.
    """
    a, b = _data(session, "a"), _data(session, "b")
    _version(session, a, NOW)
    flow = _flow(session, [a, b], fm.TriggerEnum.ANY_INPUT)

    assert flow._has_new_input(require_all=False) is False


def test_all_requires_every_input_newer_than_the_last_run(session):
    a, b = _data(session, "a"), _data(session, "b")
    _version(session, a, EARLIER)
    _version(session, b, EARLIER)
    flow = _flow(session, [a, b], fm.TriggerEnum.ALL_INPUT)
    flow.last_executed = NOW

    _version(session, a, LATER, checksum="c2")     # only A refreshed
    assert flow._has_new_input(require_all=True) is False

    _version(session, b, LATER, checksum="c2")     # now both have
    assert flow._has_new_input(require_all=True) is True


def test_any_fires_on_the_first_input_to_refresh(session):
    a, b = _data(session, "a"), _data(session, "b")
    _version(session, a, EARLIER)
    _version(session, b, EARLIER)
    flow = _flow(session, [a, b], fm.TriggerEnum.ANY_INPUT)
    flow.last_executed = NOW

    assert flow._has_new_input(require_all=False) is False

    _version(session, a, LATER, checksum="c2")
    assert flow._has_new_input(require_all=False) is True


def test_a_first_run_with_every_input_populated_goes_ahead(session):
    """Registering against sources that already have data still runs at once."""
    a, b = _data(session, "a"), _data(session, "b")
    _version(session, a, NOW)
    _version(session, b, NOW)
    flow = _flow(session, [a, b], fm.TriggerEnum.ALL_INPUT)

    assert flow.last_executed is None
    assert flow._has_new_input(require_all=True) is True


def test_a_flow_with_no_inputs_never_fires(session):
    flow = _flow(session, [], fm.TriggerEnum.ANY_INPUT)

    assert flow._has_new_input(require_all=False) is False
    assert flow._has_new_input(require_all=True) is False
