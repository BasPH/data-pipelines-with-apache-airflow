import datetime

import pytest
from airflow import DAG

pytest_plugins = ["helpers_namespace"]


@pytest.fixture
def test_dag():
    """Airflow DAG for testing."""
    return DAG(
        "test_dag",
        default_args={"owner": "airflow", "start_date": datetime.datetime(2018, 1, 1)},
        schedule_interval=datetime.timedelta(days=1),
    )


@pytest.helpers.register
def run_task(task, dag):
    """Run an Airflow task."""
    dag.clear()
    task.run(
        start_date=dag.default_args["start_date"],
        end_date=dag.default_args["start_date"],
    )
