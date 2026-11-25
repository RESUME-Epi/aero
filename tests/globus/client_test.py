import pytest
import uuid

from unittest.mock import Mock
from unittest.mock import patch

from aero.globus import client


@pytest.fixture(autouse=True)
def reset_search():
    enabled = client.Config.SEARCH_ENABLED

    yield

    client.Config.SEARCH_ENABLED = enabled


@pytest.fixture
def globus_client():
    return client.GlobusClient(search_index=1234)


@pytest.fixture
def function_args():
    return {"kwargs": {"a1": "kwa1"}}


def test_client_init(globus_client):
    assert isinstance(globus_client.app, client.ClientApp)
    assert sorted(globus_client.specific_flow_clients.keys()) == sorted(
        client.FLOW_IDS.values()
    )
    assert isinstance(globus_client.auth_client, client.AuthClient)
    assert isinstance(globus_client.search_client, client.SearchClient)
    assert globus_client.search_index == 1234


def test_create_search_idx(globus_client, capsys):
    # test when search index already exists
    globus_client._create_search_idx()
    captured = capsys.readouterr()
    assert captured.out == ""

    # test actual index creation
    globus_client.search_index = None
    globus_client._create_search_idx()
    captured = capsys.readouterr()

    assert captured.out == "Created new search index: index\n"


def test_timer_scopes(globus_client):
    timer_scopes = globus_client._timer_scopes()
    for sfc in globus_client.specific_flow_clients.values():
        assert sfc.scopes.user in timer_scopes[0].dependencies


def test_add_search_entry(globus_client, caplog):
    # search enabled and passes
    entry = {"subject": "fake_entry", "visible_to": ["public"], "content": {}}
    assert globus_client.add_search_entry(entry) == "ingest complete"

    with patch("aero.globus.client.SearchClient.ingest") as mock_sc:
        http_response = Mock()
        http_response.status_code = 404
        failure_text = "mock ingest searchapierror"
        http_response.text = f'{{"message": "{failure_text}"}}'
        http_response.json.return_value = {"message": failure_text}
        http_response.headers = {"Content-Type": "application/json"}
        http_response.request.headers = {"Authorization": "Bearer 1234"}

        # ingest throws SearchApiError
        mock_sc.side_effect = client.SearchAPIError(http_response)
        result = globus_client.add_search_entry(entry)

        with caplog.at_level("WARNING"):
            assert result is None
            assert (
                f"Globus Search ingest failed ({http_response.status_code}), continuing without indexing: {failure_text}"
                in caplog.text
            )

            # ingest throws another exception
            mock_exception = Exception("other exception")
            mock_sc.side_effect = mock_exception
            result = globus_client.add_search_entry(entry)
            assert result is None
            assert (
                f"Globus Search ingest failed ({type(mock_exception).__name__}), continuing without indexing: {mock_exception}"
                in caplog.text
            )

    with caplog.at_level("DEBUG"):
        client.Config.SEARCH_ENABLED = False
        assert globus_client.add_search_entry(entry) is None
        assert "Globus Search disabled" in caplog.text


def test_get_user_uuid(globus_client):
    assert globus_client.get_user_uuid("mock_username") == "myid"


def test_run_flow(globus_client):
    example_task_complete = {
        "kwargs": {
            "aero": {
                "input_data": {
                    "data_1": {"id": 1},
                }
            }
        }
    }

    # test with all fields populated
    assert (
        globus_client.run_flow(
            id=1,
            endpoint_uuid="2",
            function_uuid="3",
            pull_function_uuid="4",
            commit_function_uuid="5",
            tasks=example_task_complete,
            email="email@email.com",
            trigger_url="trigger.url",
            signed_url="signed.url",
            source_data_id=1,
        )
        is None
    )

    # test with tasks in a list
    assert (
        globus_client.run_flow(
            id=1,
            endpoint_uuid="2",
            function_uuid="3",
            pull_function_uuid="4",
            commit_function_uuid="5",
            tasks=[example_task_complete],
            email="email@email.com",
            trigger_url="trigger.url",
            signed_url="signed.url",
            source_data_id=1,
        )
        is None
    )

    # test where signed URL is None
    assert (
        globus_client.run_flow(
            id=1,
            endpoint_uuid="2",
            function_uuid="3",
            pull_function_uuid="4",
            commit_function_uuid="5",
            tasks=example_task_complete,
            email="email@email.com",
            trigger_url="trigger.url",
            source_data_id=1,
        )
        is None
    )

    # test when source ID does not correspond to id
    example_task_complete["kwargs"]["aero"]["input_data"]["data_1"]["id"] = 2
    assert (
        globus_client.run_flow(
            id=1,
            endpoint_uuid="2",
            function_uuid="3",
            pull_function_uuid="4",
            commit_function_uuid="5",
            tasks=example_task_complete,
            email="email@email.com",
            trigger_url="trigger.url",
            signed_url="signed.url",
            source_data_id=1,
        )
        is None
    )

    # test when trigger_url, source_data_id and signed_url are not provided
    assert (
        globus_client.run_flow(
            id=1,
            endpoint_uuid="2",
            function_uuid="3",
            pull_function_uuid="4",
            commit_function_uuid="5",
            tasks=example_task_complete,
            email="email@email.com",
        )
        is None
    )

    # test when email is None
    assert (
        globus_client.run_flow(
            id=1,
            endpoint_uuid="2",
            function_uuid="3",
            pull_function_uuid="4",
            commit_function_uuid="5",
            tasks=example_task_complete,
            email=None,
        )
        is None
    )


def test_set_timer(globus_client, function_args):
    # test user flow
    assert isinstance(
        globus_client.set_timer(
            interval_in_sec=1,
            id=1,
            email=None,
            flow_type=client.FlowEnum.USER_FLOW,
            user_function=str(uuid.uuid4()),
            pull_function_uuid=str(uuid.uuid4()),
            commit_function_uuid=str(uuid.uuid4()),
            function_args=function_args,
            user_endpoint=str(uuid.uuid4()),
        ),
        uuid.UUID,
    )

    # test verify and modify
    assert isinstance(
        globus_client.set_timer(
            interval_in_sec=1,
            id=1,
            email="email@email.com",
            flow_type=client.FlowEnum.VERIFY_AND_MODIFY,
            user_function=str(uuid.uuid4()),
            pull_function_uuid=str(uuid.uuid4()),
            commit_function_uuid=str(uuid.uuid4()),
            function_args=function_args,
            user_endpoint=str(uuid.uuid4()),
        ),
        uuid.UUID,
    )


def test_run_ingestion_flow(globus_client, function_args):
    assert (
        globus_client.run_ingestion_flow(
            id=1,
            email=None,
            user_function=str(uuid.uuid4()),
            pull_function_uuid=str(uuid.uuid4()),
            commit_function_uuid=str(uuid.uuid4()),
            function_args=function_args,
            user_endpoint=str(uuid.uuid4()),
            source_url="source_url",
            source_key="source_key",
            dedup=False,
        )
        is None
    )

    # with email preset and source_url and key unset
    assert (
        globus_client.run_ingestion_flow(
            id=1,
            email="email@email.com",
            user_function=str(uuid.uuid4()),
            pull_function_uuid=str(uuid.uuid4()),
            commit_function_uuid=str(uuid.uuid4()),
            function_args=function_args,
            user_endpoint=str(uuid.uuid4()),
            dedup=True,
        )
        is None
    )


def test_delete_job(globus_client):
    assert globus_client.delete_job(job_id=1234) is None
