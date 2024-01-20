from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pendulum import datetime


def _calculate_stats(**context):
    """Calculates event statistics."""
    input_path = context["templates_dict"]["input_path"]
    output_path = context["templates_dict"]["output_path"]

    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index()

    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)


def email_stats(stats, email):
    """Send an email..."""
    print(f"Sending stats to {email}...{stats}")


def _send_stats(email, **context):
    stats = pd.read_csv(context["templates_dict"]["stats_path"])
    email_stats(stats, email=email)


with DAG(
    dag_id="14_atomic_send",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 5),
):
    fetch_events = BashOperator(
        task_id="fetch_events",
        bash_command=(
            "curl -o /data/events/{{data_interval_start | ds}}.json "
            "http://events_api:5000/events?"
            "start_date={{data_interval_start | ds}}&"
            "end_date={{data_interval_end | ds}}"
        ),
    )

    calculate_stats = PythonOperator(
        task_id="calculate_stats",
        python_callable=_calculate_stats,
        templates_dict={
            "input_path": "/data/events/{{data_interval_start | ds}}.json",
            "output_path": "/data/stats/{{data_interval_start | ds}}.csv",
        },
    )

    send_stats = PythonOperator(
        task_id="send_stats",
        python_callable=_send_stats,
        op_kwargs={"email": "user@example.com"},
        templates_dict={
            "stats_path": "/data/stats/{{data_interval_start | ds}}.csv",
        },
    )

    fetch_events >> calculate_stats >> send_stats
