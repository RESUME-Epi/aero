import pytest
import uuid

from dataclasses import dataclass
from typing import Any
from unittest import mock

from globus_sdk import scopes

from sqlmodel import Session
from sqlmodel import create_engine
from sqlmodel import SQLModel
from sqlmodel.pool import StaticPool

from aero import init_client
from aero.globus.utils import FlowEnum
from aero.globus.utils import FLOW_IDS
from aero.globus.client import SearchAPIError

engine = create_engine(
    "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
)
patcher = mock.patch("sqlmodel.create_engine", return_value=engine)
patcher.start()


@pytest.fixture(name="globus_mock", autouse=True)
def _mock_globus(monkeypatch):
    class MockClientApp:
        def __init__(*args, **kwargs):
            pass

    class MockAuthClient:
        def __init__(*args, **kwargs):
            pass

        def get_auth_identity(usernames):
            return {"identities": [{"id": "myid"}]}

    class MockSearchClient:
        def __init__(self, *args, **kwargs):
            self.scopes = scopes.SearchScopes

        def create_search_index(*args, **kwargs):
            return {"id": "index"}

        def ingest(index, *args, **kwargs):
            @dataclass
            class Response:
                text: str

            if index is None:
                raise SearchAPIError()
            return Response("ingest complete")

        def add_search_entry(self, entry):
            return "entry has been added"

        def add_app_scope(self, scope):
            return "scope has been added"

    class MockSpecificFlowClient:
        def __init__(self, *args, **kwargs):
            self.scopes = scopes.SpecificFlowScopes(FLOW_IDS[FlowEnum.USER_FLOW])

        def run_flow(*args, **kwargs):
            @dataclass
            class Response:
                http_status = 201

            return Response()

        def delete_job(*args, **kwargs):
            @dataclass
            class Response:
                http_status = 201

            return Response()

    class MockSpecificTimerClient:
        def __init__(*args, **kwargs):
            pass

        def set_timer(*args, **kwargs):
            return uuid.uuid4()

        def create_timer(*args, **kwargs):
            @dataclass
            class Response:
                http_status = 201
                timer = {"job_id": str(uuid.uuid4())}

                def __getitem__(self, key: int | str) -> Any:
                    if isinstance(key, int):
                        return list(self.__dict__.values())[key]
                    elif isinstance(key, str):
                        return getattr(self, key)
                    else:
                        raise TypeError(
                            "Index must be an integer or a string (field name)"
                        )

            return Response()

    with (
        mock.patch("aero.globus.client.ClientApp", new=MockClientApp),
        mock.patch("aero.globus.client.SearchClient", new=MockSearchClient),
        mock.patch("aero.globus.client.TimersClient", new=MockSpecificTimerClient),
        mock.patch("aero.globus.client.SpecificFlowClient", new=MockSpecificFlowClient),
        mock.patch("aero.globus.client.AuthClient", new=MockAuthClient),
    ):
        init_client()
        yield


@pytest.fixture(name="session")
def session_fixture():
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    SQLModel.metadata.drop_all(engine)
    patcher.stop()


# @pytest.fixture(scope="session")
# def app():
#     app = create_app()
#     app.config.update({"TESTING": True})
#     with app.app_context():
#         db.drop_all()
#         db.create_all()
#         yield app
#         db.session.remove()
#         db.drop_all()


# def client(app):
#     return app.test_client()
