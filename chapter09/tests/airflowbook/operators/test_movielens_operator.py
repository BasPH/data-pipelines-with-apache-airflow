import datetime
import os
from collections import namedtuple
from pathlib import Path

import pytest
from airflow.models import DAG, Connection
from pytest_docker_tools import fetch, container
from pytest_mock import MockFixture

from airflowbook.operators.movielens_operator import (
    MovielensDownloadOperator,
    MovielensHook,
    MovielensToPostgresOperator,
    PostgresHook,
)


@pytest.fixture(scope="module")
def postgres_credentials():
    PostgresCredentials = namedtuple("PostgresCredentials", ["username", "password"])
    return PostgresCredentials("testuser", "testpass")


postgres_image = fetch(repository="postgres:11.1-alpine")
postgres = container(
    image="{postgres_image.id}",
    environment={
        "POSTGRES_USER": "{postgres_credentials.username}",
        "POSTGRES_PASSWORD": "{postgres_credentials.password}",
    },
    ports={"5432/tcp": None},
    volumes={
        os.path.join(os.path.dirname(__file__), "postgres-init.sql"): {
            "bind": "/docker-entrypoint-initdb.d/postgres-init.sql"
        }
    },
)


def test_movielens_operator(tmp_path: Path, mocker: MockFixture):
    mocker.patch.object(
        MovielensHook,
        "get_connection",
        return_value=Connection(conn_id="test", login="airflow", password="airflow"),
    )
    dag = DAG(
        "test_dag",
        default_args={"owner": "airflow", "start_date": datetime.datetime(2019, 1, 1)},
        schedule_interval="@daily",
    )

    task = MovielensDownloadOperator(
        task_id="test",
        conn_id="testconn",
        start_date="{{ prev_ds }}",
        end_date="{{ ds }}",
        output_path=str(tmp_path / "{{ ds }}.json"),
        dag=dag,
    )

    dag.clear()
    task.run(
        start_date=dag.default_args["start_date"],
        end_date=dag.default_args["start_date"],
        ignore_ti_state=True,
    )


def test_movielens_to_postgres_operator(
    mocker: MockFixture, test_dag: DAG, postgres, postgres_credentials
):
    mocker.patch.object(
        MovielensHook,
        "get_connection",
        return_value=Connection(conn_id="test", login="airflow", password="airflow"),
    )
    mocker.patch.object(
        PostgresHook,
        "get_connection",
        return_value=Connection(
            conn_id="postgres",
            conn_type="postgres",
            host="localhost",
            login=postgres_credentials.username,
            password=postgres_credentials.password,
            port=postgres.ports["5432/tcp"][0],
        ),
    )

    task = MovielensToPostgresOperator(
        task_id="test",
        movielens_conn_id="movielens_id",
        start_date="{{ prev_ds }}",
        end_date="{{ ds }}",
        postgres_conn_id="postgres_id",
        insert_query=(
            "INSERT INTO movielens (movieId,rating,ratingTimestamp,userId,scrapeTime) "
            "VALUES ({0}, '{{ macros.datetime.now() }}')"
        ),
        dag=test_dag,
    )

    pg_hook = PostgresHook()

    row_count = pg_hook.get_first("SELECT COUNT(*) FROM movielens")[0]
    assert row_count == 0

    pytest.helpers.run_airflow_task(task, test_dag)

    row_count = pg_hook.get_first("SELECT COUNT(*) FROM movielens")[0]
    assert row_count > 0


postgres_container = container(image="{postgres_image.id}", ports={"5432/tcp": None})


def test_call_fixture(postgres_container):
    print(
        f"Running Postgres container named {postgres_container.name} "
        f"on port {postgres_container.ports['5432/tcp'][0]}."
    )
