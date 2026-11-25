import globus_sdk
import hashlib
import pytest
import time

from unittest.mock import patch

from aero.globus import auth
from aero import Config

REQUIRED_SCOPE = (
    Config.AUTH_SCOPE
    or f"https://auth.globus.org/scopes/{Config.PORTAL_CLIENT_ID}/action_all"
)


@pytest.fixture(autouse=True)
def reset_auth_req():
    auth_req = Config.REQUIRE_AUTH

    yield

    Config.REQUIRE_AUTH = auth_req


def test_required_scope():
    assert auth._required_scope() == REQUIRED_SCOPE


def test_get_introspect_client(monkeypatch):
    # execute if statement if False
    monkeypatch.setattr(auth, "_introspect_client", None)

    assert isinstance(
        auth._get_introspect_client(), globus_sdk.ConfidentialAppAuthClient
    )
    assert isinstance(auth._introspect_client, globus_sdk.ConfidentialAppAuthClient)

    # check that if statement is skipped if _introspect_client is set
    prev_client = auth._introspect_client

    auth._get_introspect_client()

    assert prev_client == auth._introspect_client


@patch("aero.globus.auth.globus_sdk.ConfidentialAppAuthClient.oauth2_token_introspect")
def test_token_is_valid(mock_introspect):
    # pass cached token test
    token = "fake_token"
    auth._introspect_cache = {
        hashlib.sha256(token.encode()).hexdigest(): time.monotonic()
    }
    assert auth._token_is_valid(token)

    # token is not active
    mock_introspect.return_value = {"active": False, "scope": ""}
    assert not auth._token_is_valid("token_not_active")

    # token has an invalid scope
    mock_introspect.return_value = {"active": True, "scope": ""}
    assert not auth._token_is_valid("token_invalid_scope")

    # token is active and has the right scope
    mock_introspect.return_value = {"active": True, "scope": REQUIRED_SCOPE}
    assert auth._token_is_valid("valid_token")


def test_require_globus_auth():
    # Auth is not required
    Config.REQUIRE_AUTH = False
    assert auth.require_globus_auth() is None

    # Auth is required but creds are not set
    Config.REQUIRE_AUTH = True

    # when creds are None
    with pytest.raises(auth.HTTPException):
        auth.require_globus_auth(creds=None)

    # evaluating with empty credentials
    mock_creds = auth.HTTPAuthorizationCredentials(scheme="Bearer", credentials="")

    with pytest.raises(auth.HTTPException):
        auth.require_globus_auth(creds=mock_creds)

    # evaluating with valid credentials
    mock_creds = auth.HTTPAuthorizationCredentials(
        scheme="Bearer", credentials="mock_token"
    )

    with pytest.raises(auth.HTTPException):
        auth.require_globus_auth(creds=mock_creds)

    # valid credentials
    auth._introspect_cache = {
        hashlib.sha256(mock_creds.credentials.encode()).hexdigest(): time.monotonic()
    }
    assert auth.require_globus_auth(creds=mock_creds) == mock_creds.credentials
