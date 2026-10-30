"""Is a Globus Compute endpoint online?

Used to pick between a flow's primary endpoint and its configured failovers
before a run is submitted. Submitting to a dead endpoint succeeds as far as
Globus Flows is concerned — the run only fails later, by which time
``Flow.last_executed`` has already moved past the data that triggered it.

Three-valued on purpose. ``None`` means the question could not be answered (no
permission on the endpoint, a timeout, a network failure), and is never a reason
to fail over: an unreachable *status service* says nothing about an endpoint, and
treating it as "down" would reroute every run the moment Globus Auth hiccuped.
Only an explicit "offline" moves a run.
"""

import logging
import time

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from uuid import UUID

from aero.config import Config

logger = logging.getLogger(__name__)

# endpoint uuid -> (monotonic deadline, verdict). The check sits inline in the
# notify webhook and in POST /prov/new, so a burst of notifies against one
# endpoint must not become a burst of Globus calls.
_CACHE: dict[str, tuple[float, bool | None]] = {}

# An unknown verdict is held for a fraction of the normal TTL: it usually means
# something transient, and re-asking sooner costs little.
_UNKNOWN_TTL_FRACTION = 0.25

_client = None
_executor: ThreadPoolExecutor | None = None


def _compute_client():
    """The Globus Compute client used for status lookups, built on first use.

    Never at import: ``aero/__init__`` already constructs a ``GlobusClient`` when
    the package loads, and a second network-dependent client there would make
    every test and every startup pay for it.

    It gets its own ``ClientApp`` rather than sharing ``GLOBUS_CLIENT.app`` so
    that adding Compute scopes cannot disturb the token state the flows and
    timers clients depend on. The credential preferred is
    ``FUNCX_SDK_CLIENT_ID``/``_SECRET`` — endpoints are normally started with it,
    and an endpoint's status only answers to a caller with a role on it.
    """
    global _client

    if _client is None:
        from globus_compute_sdk import Client as ComputeClient
        from globus_sdk import ClientApp

        app = ClientApp(
            "aero-endpoint-health",
            client_id=Config.CLIENT_ID or Config.PORTAL_CLIENT_ID,
            client_secret=Config.CLIENT_SECRET or Config.PORTAL_CLIENT_SECRET,
        )
        # do_version_check defaults True and makes a web call of its own.
        _client = ComputeClient(app=app, do_version_check=False)

    return _client


def _get_executor() -> ThreadPoolExecutor:
    global _executor

    if _executor is None:
        _executor = ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="aero-endpoint-health"
        )

    return _executor


def _query_status(endpoint_uuid: str) -> bool | None:
    """Ask Globus Compute, bounded by ``AERO_ENDPOINT_CHECK_TIMEOUT``.

    The deadline is applied by waiting on a worker thread rather than by setting
    the SDK's transport timeout, which lives behind a private attribute.
    """
    try:
        future = _get_executor().submit(
            lambda: _compute_client().get_endpoint_status(endpoint_uuid)
        )
        response = future.result(timeout=Config.ENDPOINT_CHECK_TIMEOUT)
    except FutureTimeoutError:
        logger.warning(
            "endpoint %s: status check timed out after %ss, treating as unknown "
            "(an unknown endpoint is used, not failed over from)",
            endpoint_uuid,
            Config.ENDPOINT_CHECK_TIMEOUT,
        )
        return None
    except Exception as e:
        # Most often a 403: the status of an endpoint only answers to a caller
        # with a role on it. Named explicitly because the symptom otherwise is
        # that failover silently never fires.
        logger.warning(
            "endpoint %s: status check failed (%s: %s), treating as unknown -- if "
            "this persists, the AERO credential cannot see this endpoint and "
            "failover will never trigger",
            endpoint_uuid,
            type(e).__name__,
            e,
        )
        return None

    status = (response or {}).get("status")
    if status == "online":
        return True
    if status == "offline":
        return False

    logger.warning(
        "endpoint %s: unrecognized status %r, treating as unknown",
        endpoint_uuid,
        status,
    )
    return None


def endpoint_is_up(endpoint_uuid: UUID | str, use_cache: bool = True) -> bool | None:
    """Whether ``endpoint_uuid`` is online.

    ``True`` online, ``False`` offline, ``None`` could not tell. ``use_cache``
    False forces a live lookup, which is what the status route uses so an
    operator can confirm the check works rather than reading a stale verdict.
    """
    key = str(endpoint_uuid)
    now = time.monotonic()

    if use_cache:
        cached = _CACHE.get(key)
        if cached is not None and cached[0] > now:
            return cached[1]

    verdict = _query_status(key)

    ttl = Config.ENDPOINT_CHECK_TTL
    if ttl > 0:
        if verdict is None:
            ttl = ttl * _UNKNOWN_TTL_FRACTION
        _CACHE[key] = (now + ttl, verdict)

    return verdict


def clear_cache() -> None:
    """Drop every cached verdict. For tests and for the status route."""
    _CACHE.clear()
