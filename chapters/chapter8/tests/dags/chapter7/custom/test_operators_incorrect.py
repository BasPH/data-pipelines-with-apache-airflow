from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection
from pytest_mock import MockFixture

from airflowbook.operators.movielens_operator import MovielensPopularityOperator


def test_movielenspopularityoperator(mocker: MockFixture):
    mock_get = mocker.patch.object(
        BaseHook,
        "get_connection",
        return_value=Connection(conn_id="test", login="airflow", password="airflow"),
    )
    task = MovielensPopularityOperator(
        task_id="test_id",
        conn_id="testconn",
        start_date="2015-01-01",
        end_date="2015-01-03",
        top_n=5,
    )
    result = task.execute(context=None)
    assert len(result) == 5
    mock_get.assert_called_once_with("testconn")
