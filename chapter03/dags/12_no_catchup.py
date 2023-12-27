from datetime import datetime
from pathlib import Path

import pandas as pd

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def _calculate_stats(**context):
    """Calculates event statistics."""
    input_path = context["templates_dict"]["input_path"]
    output_path = context["templates_dict"]["output_path"]

    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index()

    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)


with DAG(
    dag_id="12_no_catchup",
    schedule="@daily",
    start_date=datetime(2019,1,1),
    end_date=datetime(2019,1,5),
    catchup=False,
):

    fetch_events = BashOperator(
        task_id="fetch_events",
        bash_command=(
            "mkdir -p /data/events && "
            "curl -o /data/events/{{ds}}.json "
            "http://events_api:5000/events?"
            "start_date={{ds}}&"
            "end_date={{next_ds}}"
        ),
    )

    calculate_stats = PythonOperator(
        task_id="calculate_stats",
        python_callable=_calculate_stats,
        templates_dict={
            "input_path": "/data/events/{{ds}}.json",
            "output_path": "/data/stats/{{ds}}.csv",
        },
    )


    fetch_events >> calculate_stats
