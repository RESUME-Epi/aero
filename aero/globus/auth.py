"""FastAPI authentication dependency for the AERO server.

Validates a Globus Auth bearer token (the ``action_all`` scope for this server's
resource server) via token introspection. Enabled only when ``AERO_REQUIRE_AUTH``
is set; otherwise the dependency is a no-op, so auth can be rolled out per
deployment without code changes.
"""

import hashlib
import time

import globus_sdk

from fastapi import Depends
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from fastapi.security import HTTPBearer

from aero.config import Config


_bearer_scheme = HTTPBearer(auto_error=False)

# Token introspection is a network round-trip; cache validated tokens briefly.
_INTROSPECT_TTL_SECONDS = 60
_introspect_cache: dict[str, float] = {}

_introspect_client: globus_sdk.ConfidentialAppAuthClient | None = None


def _required_scope() -> str:
    return Config.AUTH_SCOPE or (
        f"https://auth.globus.org/scopes/{Config.PORTAL_CLIENT_ID}/action_all"
    )


def _get_introspect_client() -> globus_sdk.ConfidentialAppAuthClient:
    # Introspection requires the resource server's confidential credentials.
    global _introspect_client
    if _introspect_client is None:
        _introspect_client = globus_sdk.ConfidentialAppAuthClient(
            Config.PORTAL_CLIENT_ID, Config.PORTAL_CLIENT_SECRET
        )
    return _introspect_client


def _token_is_valid(token: str) -> bool:
    """Introspect a bearer token: True iff active and carrying the AERO scope."""
    key = hashlib.sha256(token.encode()).hexdigest()
    now = time.monotonic()
    cached = _introspect_cache.get(key)
    if cached is not None and now - cached < _INTROSPECT_TTL_SECONDS:
        return True

    meta = _get_introspect_client().oauth2_token_introspect(token)
    if not meta.get("active"):
        return False
    if _required_scope() not in (meta.get("scope") or "").split():
        return False

    _introspect_cache[key] = now  # cache positives only
    return True


def require_globus_auth(
    creds: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
):
    """Dependency enforcing a valid, AERO-scoped Globus token.

    No-op unless ``Config.REQUIRE_AUTH`` is set (safe incremental rollout).
    Raises 401 for a missing token and 403 for an invalid / wrongly-scoped one.
    """
    if not Config.REQUIRE_AUTH:
        return None

    if creds is None or not creds.credentials:
        raise HTTPException(status_code=401, detail="Missing bearer token.")

    if not _token_is_valid(creds.credentials):
        raise HTTPException(
            status_code=403, detail="Invalid or insufficiently scoped token."
        )
    return creds.credentials
