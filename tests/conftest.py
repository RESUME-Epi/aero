import pytest
import uuid

from dataclasses import dataclass
from typing import Any
from unittest import mock

from sqlmodel import Session
from sqlmodel import create_engine
from sqlmodel import SQLModel
from sqlmodel.pool import StaticPool

from aero.globus import client
from aero.globus.client import SearchAPIError

engine = create_engine(
    "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
)
patcher = mock.patch("sqlmodel.create_engine", return_value=engine)
patcher.start()


@pytest.fixture(name="globus_mock", autouse=True)
def _mock_globus(monkeypatch):
    def add_search_entry(self, entry):
        return "entry has been added"

    def create_search_index(*args, **kwargs):
        return {"id": "index"}

    def search_ingest(index, *args, **kwargs):
        @dataclass
        class Response:
            text: str

        if index is None:
            raise SearchAPIError()
        return Response("ingest complete")

    def get_auth_identity(usernames):
        return {"identities": [{"id": "myid"}]}

    class MockGlobusClient:
        def __init__(self, search_idx):
            pass

    class MockSpecificFlowClient:
        def run_flow(*args, **kwargs):
            @dataclass
            class Response:
                http_status = 201

            return Response()

    class MockSpecificTimerClient:
        def create_job(*args, **kwargs):
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

    monkeypatch.setattr(client.GlobusClient, "__init__", MockGlobusClient.__init__)
    monkeypatch.setattr(
        client.SearchClient,
        "create_index",
        create_search_index,
    )
    monkeypatch.setattr(client.SearchClient, "ingest", search_ingest)
    monkeypatch.setattr(client.AuthClient, "get_identities", get_auth_identity)
    monkeypatch.setattr(
        client.SpecificFlowClient,
        "run_flow",
        MockSpecificFlowClient.run_flow,
    )

    monkeypatch.setattr(
        client.TimersClient, "create_timer", MockSpecificTimerClient.create_job
    )
    yield


@pytest.fixture(name="session")
def session_fixture():
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session
    SQLModel.metadata.drop_all(engine)
    patcher.stop()


# # dsc_mock = mock.patch("osprey.server.lib.globus_search.DSaaSSearchClient")
# # st_mock = mock.patch("osprey.server.jobs.timer.set_timer", return_value=1111)
# # rf_mock = mock.patch("osprey.server.jobs.user_flow.run_flow")

# from aero.main import create_app


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


# # @pytest.fixture()
# @pytest.fixture(scope="session", autouse=True)
# def _mock_globus():
#     with (
#         mock.patch("aero.automate.timer.set_timer", return_value=1111) as _,
#         mock.patch("aero.automate.policy.run_flow") as _,
#         mock.patch("aero.globus.search.DSaaSSearchClient", autospec=True) as _,
#     ):
#         yield


# def client(app):
#     return app.test_client()
