import pytest

from unittest import mock
from uuid import uuid4

from pydantic import BaseModel

from fastapi.testclient import TestClient

from typing import Any

from sqlmodel import Session

import aero
import aero.models


class MockedAuthClient(BaseModel):
    authorizer: Any

    def get_identities(self, usernames: str):
        return {"identities": [{"id": 1}]}


@pytest.fixture(name="client")
def client_fixture(session: Session):
    import aero.main

    def get_session_override():
        return session

    def run_flow_override(*args, **kwargs):
        pass

    from aero.database import get_session

    aero.main.app.dependency_overrides[get_session] = get_session_override
    aero.main.app.dependency_overrides["run_flow"] = run_flow_override
    aero.main.app.router.lifespan_context = aero.main.lifespan

    client = TestClient(aero.main.app)
    yield client
    aero.main.app.dependency_overrides.clear()


@pytest.fixture(name="data")
def data_fixture(session: Session):
    data = aero.models.data.create_data(
        session=session,
        name="Test data",
        url="https://test.com",
        collection_url="https://globus.org/test",
        collection_uuid=uuid4(),
        description="",
    )

    version = aero.models.data_version.create_dataversion(
        session=session, version=1, checksum="123", data_id=data.id
    )

    _ = aero.models.data_file.create_datafile(
        session=session, file_name="testfile", size=2, version_id=version.id
    )
    return data


@pytest.fixture(name="noversion_data")
def noversion_data_fixture(session: Session):
    data = aero.models.data.create_data(
        session=session,
        name="Test data",
        url="https://test.com",
        collection_url="https://globus.org/test",
        collection_uuid=uuid4(),
        description="",
    )

    return data


@pytest.fixture(name="flow")
def flow_fixture(session: Session, data, noversion_data):
    func = aero.models.function.create_function(session=session, uuid=uuid4())
    p_func = aero.models.function.create_function(session=session, uuid=uuid4())
    c_func = aero.models.function.create_function(session=session, uuid=uuid4())
    flow = aero.models.flows.create_flow(
        session=session,
        derived_from=[data],
        contributed_to=[noversion_data],
        endpoint=uuid4(),
        function_id=func.id,
        pull_function_id=p_func.id,
        commit_function_id=c_func.id,
    )

    return flow


@pytest.fixture(name="prov")
def prov_fixture(session: Session, data, noversion_data, flow):
    prov = aero.models.provenance.create_provenance(
        session=session,
        flow_id=flow.id,
        derived_from=[data.last_version()],
    )

    return prov


@pytest.fixture(scope="session", autouse=True)
def _mock_auth():
    with mock.patch("aero.auth._token_is_valid", return_value=True) as _:
        yield
