from uuid import uuid4

import pytest

import aero.models.data
import aero.routers.data as data_router

from aero.routers.data import _normalize_full_url
from aero.routers.data import _normalize_object_key


ROUTE = "/data/notify"


@pytest.mark.parametrize(
    "value, expected",
    [
        # full presigned URL: scheme/host/port/query all dropped
        (
            "http://localhost:9000/test-bucket/lhs_results.csv?X-Amz-Signature=abc",
            "test-bucket/lhs_results.csv",
        ),
        # plain object URL, different host -> same key
        (
            "http://127.0.0.1:9000/test-bucket/lhs_results.csv",
            "test-bucket/lhs_results.csv",
        ),
        # bare bucket/key
        ("test-bucket/lhs_results.csv", "test-bucket/lhs_results.csv"),
        # trailing slash
        ("https://minio.internal/traffic/report.xml.gz/", "traffic/report.xml.gz"),
        # nested key
        ("https://minio.internal/bucket/a/b/c.csv", "bucket/a/b/c.csv"),
        # leading slash on a bare path
        ("/bucket/key.csv", "bucket/key.csv"),
    ],
)
def test_normalize_object_key(value, expected):
    assert _normalize_object_key(value) == expected


def test_normalize_object_key_equivalence():
    """A presigned URL, a plain URL, and a bare bucket/key all normalize equal."""

    presigned = "https://localhost:9000/traffic/report.xml.gz?X-Amz-Expires=3600"
    plain = "http://127.0.0.1:9000/traffic/report.xml.gz"
    bare = "traffic/report.xml.gz"
    assert (
        _normalize_object_key(presigned)
        == _normalize_object_key(plain)
        == _normalize_object_key(bare)
    )


@pytest.mark.parametrize(
    "value, expected",
    [
        # keeps host+port, drops scheme and query
        (
            "https://127.0.0.1:9000/dup-bucket/file.csv?X-Amz-Sig=z",
            "127.0.0.1:9000/dup-bucket/file.csv",
        ),
        (
            "http://127.0.0.1:9000/dup-bucket/file.csv",
            "127.0.0.1:9000/dup-bucket/file.csv",
        ),
        # trailing slash trimmed
        ("http://minio.other:9000/b/k/", "minio.other:9000/b/k"),
        # bare bucket/key has no host -> just the key (won't match a host-qualified url)
        ("dup-bucket/file.csv", "dup-bucket/file.csv"),
    ],
)
def test_normalize_full_url(value, expected):
    assert _normalize_full_url(value) == expected


def test_normalize_full_url_http_https_equal():
    assert _normalize_full_url("http://h:9000/b/k") == _normalize_full_url(
        "https://h:9000/b/k?sig=1"
    )


class _Spy:
    """Stand-in for _run_event_ingestion; records how it was called."""

    def __init__(self):
        self.calls = []

    def __call__(
        self,
        session,
        data,
        trigger_url,
        signed_url,
        object_key,
        etag=None,
        size=None,
        dedup=True,
    ):
        self.calls.append(
            {
                "data": data,
                "trigger_url": trigger_url,
                "signed_url": signed_url,
                "object_key": object_key,
                "etag": etag,
                "size": size,
                "dedup": dedup,
            }
        )
        return {
            "status": "ingestion triggered",
            "flow_id": uuid4(),
            "data_id": data.id,
        }


@pytest.fixture(name="spy")
def spy_fixture(monkeypatch):
    spy = _Spy()
    monkeypatch.setattr(data_router, "_run_event_ingestion", spy)
    return spy


def _make_data(session, url):
    return aero.models.data.create_data(
        session=session,
        name="src",
        url=url,
        collection_url="https://globus.org/test",
        collection_uuid=uuid4(),
        description="",
    )


def test_notify_by_object_single_match(client, session, spy):
    d = _make_data(session, "http://127.0.0.1:9000/test-bucket/lhs_results.csv")
    _make_data(session, "http://127.0.0.1:9000/other-bucket/thing.csv")

    presigned = "http://localhost:9000/test-bucket/lhs_results.csv?X-Amz-Signature=xyz"
    resp = client.post(
        ROUTE, json={"file_id": "test-bucket/lhs_results.csv", "url": presigned}
    )

    assert resp.status_code == 200, resp.text
    assert len(spy.calls) == 1
    assert spy.calls[0]["data"].id == d.id
    # the transient presigned url is passed through as the signed url, while the
    # stable registered url is the trigger url
    assert spy.calls[0]["signed_url"] == presigned
    assert spy.calls[0]["trigger_url"] == d.url
    assert spy.calls[0]["object_key"] == "test-bucket/lhs_results.csv"


def test_notify_by_object_matches_via_full_url_file_id(client, session, spy):
    """file_id may itself be a full object URL (different host/port than registered)."""
    d = _make_data(session, "http://127.0.0.1:9000/test-bucket/lhs_results.csv")

    resp = client.post(
        ROUTE,
        json={"file_id": "https://minio.example:9000/test-bucket/lhs_results.csv"},
    )

    assert resp.status_code == 200, resp.text
    assert spy.calls[0]["data"].id == d.id
    # no url in body -> no signed url; the registered Data.url is still the trigger
    assert spy.calls[0]["signed_url"] is None
    assert spy.calls[0]["trigger_url"] == d.url


def test_notify_by_object_no_match(client, session, spy):
    _make_data(session, "http://127.0.0.1:9000/test-bucket/lhs_results.csv")

    resp = client.post(ROUTE, json={"file_id": "unknown-bucket/missing.csv"})

    assert resp.status_code == 404, resp.text
    assert spy.calls == []


def test_notify_by_object_ambiguous_bare_key(client, session, spy):
    """Two sources share a bucket/key; a bare file_id can't disambiguate -> 409."""
    _make_data(session, "http://127.0.0.1:9000/dup-bucket/file.csv")
    _make_data(session, "https://minio.other:9000/dup-bucket/file.csv")

    resp = client.post(ROUTE, json={"file_id": "dup-bucket/file.csv"})

    assert resp.status_code == 409, resp.text
    assert spy.calls == []


def test_notify_by_object_ambiguous_resolved_by_host(client, session, spy):
    """Same bucket/key on two hosts; a host-qualified file_id breaks the tie."""
    d1 = _make_data(session, "http://127.0.0.1:9000/dup-bucket/file.csv")
    _make_data(session, "https://minio.other:9000/dup-bucket/file.csv")

    # http vs https and a presigned query must not defeat the host+port+path match
    resp = client.post(
        ROUTE,
        json={"file_id": "https://127.0.0.1:9000/dup-bucket/file.csv?X-Amz-Sig=z"},
    )

    assert resp.status_code == 200, resp.text
    assert len(spy.calls) == 1
    assert spy.calls[0]["data"].id == d1.id


def test_notify_by_object_ambiguous_unknown_host(client, session, spy):
    """Host-qualified file_id whose host matches neither source stays 409."""
    _make_data(session, "http://127.0.0.1:9000/dup-bucket/file.csv")
    _make_data(session, "https://minio.other:9000/dup-bucket/file.csv")

    resp = client.post(
        ROUTE, json={"file_id": "http://minio.third:9000/dup-bucket/file.csv"}
    )

    assert resp.status_code == 409, resp.text
    assert spy.calls == []


def test_notify_by_object_requires_file_id(client, spy):
    resp = client.post(ROUTE, json={})
    assert resp.status_code == 422  # pydantic: file_id is required
