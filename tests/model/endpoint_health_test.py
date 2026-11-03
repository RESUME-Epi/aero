import uuid

import pytest

from concurrent.futures import TimeoutError as FutureTimeoutError

from aero.config import Config
from aero.globus import health

# Captured before the autouse globus_mock fixture stubs it out: this module is
# the one place that wants the real thing, with only the SDK call underneath it
# replaced.
_REAL_QUERY_STATUS = health._query_status


@pytest.fixture(name="endpoint")
def endpoint_fixture():
    return uuid.uuid4()


@pytest.fixture(name="clock")
def clock_fixture(monkeypatch):
    """A monotonic clock the test moves by hand, so TTLs need no sleeping."""

    now = {"t": 1000.0}
    monkeypatch.setattr(health.time, "monotonic", lambda: now["t"])
    return now


@pytest.fixture(name="calls")
def calls_fixture(monkeypatch):
    """Record every status response the SDK is made to return."""

    recorded = {"n": 0, "response": {"status": "online"}, "raises": None}

    class FakeFuture:
        def result(self, timeout):
            recorded["n"] += 1
            if recorded["raises"] is not None:
                raise recorded["raises"]
            return recorded["response"]

    class FakeExecutor:
        def submit(self, fn):
            return FakeFuture()

    monkeypatch.setattr(health, "_query_status", _REAL_QUERY_STATUS)
    monkeypatch.setattr(health, "_get_executor", FakeExecutor)
    return recorded


def test_online_and_offline(endpoint, calls, clock):
    calls["response"] = {"status": "online"}
    assert health.endpoint_is_up(endpoint) is True

    health.clear_cache()
    calls["response"] = {"status": "offline"}
    assert health.endpoint_is_up(endpoint) is False


def test_unknown_status_is_none_not_false(endpoint, calls, clock):
    """An unrecognized status must not read as "down".

    Only an explicit "offline" may move a run, so anything unexpected has to
    come back None rather than falling through to a falsy verdict.
    """
    calls["response"] = {"status": "initializing"}
    assert health.endpoint_is_up(endpoint) is None

    health.clear_cache()
    calls["response"] = {}
    assert health.endpoint_is_up(endpoint) is None


def test_timeout_and_errors_are_unknown(endpoint, calls, clock):
    calls["raises"] = FutureTimeoutError()
    assert health.endpoint_is_up(endpoint) is None

    health.clear_cache()
    calls["raises"] = RuntimeError("403 forbidden")
    assert health.endpoint_is_up(endpoint) is None


def test_cache_hit_within_ttl(endpoint, calls, clock):
    calls["response"] = {"status": "online"}

    assert health.endpoint_is_up(endpoint) is True
    assert health.endpoint_is_up(endpoint) is True
    assert calls["n"] == 1, "second lookup inside the TTL should not re-ask Globus"

    clock["t"] += Config.ENDPOINT_CHECK_TTL + 1
    assert health.endpoint_is_up(endpoint) is True
    assert calls["n"] == 2


def test_unknown_is_cached_for_less_time(endpoint, calls, clock):
    """A failed check is retried sooner than a good one is refreshed."""
    calls["response"] = {"status": "nonsense"}
    assert health.endpoint_is_up(endpoint) is None
    assert calls["n"] == 1

    short = Config.ENDPOINT_CHECK_TTL * health._UNKNOWN_TTL_FRACTION
    clock["t"] += short / 2
    assert health.endpoint_is_up(endpoint) is None
    assert calls["n"] == 1

    clock["t"] += short
    assert health.endpoint_is_up(endpoint) is None
    assert calls["n"] == 2


def test_use_cache_false_always_asks(endpoint, calls, clock):
    calls["response"] = {"status": "online"}

    assert health.endpoint_is_up(endpoint) is True
    assert health.endpoint_is_up(endpoint, use_cache=False) is True
    assert calls["n"] == 2


def test_ttl_zero_disables_caching(endpoint, calls, clock, monkeypatch):
    monkeypatch.setattr(Config, "ENDPOINT_CHECK_TTL", 0)
    calls["response"] = {"status": "online"}

    health.endpoint_is_up(endpoint)
    health.endpoint_is_up(endpoint)
    assert calls["n"] == 2


def test_cache_is_per_endpoint(calls, clock):
    calls["response"] = {"status": "online"}

    health.endpoint_is_up(uuid.uuid4())
    health.endpoint_is_up(uuid.uuid4())
    assert calls["n"] == 2


def test_executor_is_built_once(monkeypatch):
    monkeypatch.setattr(health, "_executor", None)

    first = health._get_executor()

    assert first is health._get_executor()
    first.shutdown(wait=False)


def test_compute_client_prefers_the_endpoint_credential(monkeypatch):
    """Built lazily, as the confidential client that owns the endpoints.

    An endpoint's status only answers to a caller holding a role on it, so the
    FUNCX_SDK credential — the one endpoints are started with — is tried before
    the portal client. The app is its own rather than GLOBUS_CLIENT's, so adding
    Compute scopes cannot disturb the flows and timers token state.
    """
    import globus_compute_sdk
    import globus_sdk

    seen = {}

    class FakeApp:
        def __init__(self, name, client_id, client_secret):
            seen["app"] = (name, client_id, client_secret)

    class FakeClient:
        def __init__(self, app, do_version_check):
            seen["client"] = (app, do_version_check)

    monkeypatch.setattr(health, "_client", None)
    monkeypatch.setattr(globus_sdk, "ClientApp", FakeApp)
    monkeypatch.setattr(globus_compute_sdk, "Client", FakeClient)
    monkeypatch.setattr(Config, "CLIENT_ID", "funcx-id")
    monkeypatch.setattr(Config, "CLIENT_SECRET", "funcx-secret")

    built = health._compute_client()

    assert seen["app"] == ("aero-endpoint-health", "funcx-id", "funcx-secret")
    # The version check makes a web call of its own, so it stays off.
    assert seen["client"][1] is False
    assert health._compute_client() is built, "built once, then reused"


def test_compute_client_falls_back_to_the_portal_credential(monkeypatch):
    import globus_compute_sdk
    import globus_sdk

    seen = {}

    class FakeApp:
        def __init__(self, name, client_id, client_secret):
            seen["app"] = (client_id, client_secret)

    monkeypatch.setattr(health, "_client", None)
    monkeypatch.setattr(globus_sdk, "ClientApp", FakeApp)
    monkeypatch.setattr(globus_compute_sdk, "Client", lambda **kw: object())
    monkeypatch.setattr(Config, "CLIENT_ID", None)
    monkeypatch.setattr(Config, "CLIENT_SECRET", None)
    monkeypatch.setattr(Config, "PORTAL_CLIENT_ID", "portal-id")
    monkeypatch.setattr(Config, "PORTAL_CLIENT_SECRET", "portal-secret")

    health._compute_client()

    assert seen["app"] == ("portal-id", "portal-secret")
