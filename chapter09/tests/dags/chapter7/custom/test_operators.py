from airflow.models import Connection
from airflow.operators.bash import BashOperator

from airflowbook.operators.movielens_operator import (
    MovielensPopularityOperator,
    MovielensHook,
)


def test_movielenspopularityoperator(mocker):
    mock_get = mocker.patch.object(
        MovielensHook,
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
    assert mock_get.call_count == 1
    mock_get.assert_called_with("testconn")


def test_example():
    task = BashOperator(task_id="test", bash_command="echo 'hello!'", xcom_push=True)
    result = task.execute(context={})
    assert result == "hello!"
