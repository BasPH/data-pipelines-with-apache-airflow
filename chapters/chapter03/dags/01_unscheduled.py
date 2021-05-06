from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id="01_unscheduled", 
    start_date=datetime(2015, 6, 1),  # @NOTE Date/time to start scheduling DAG runs
    schedule_interval=None            # @NOTE Specify that this is an unscheduled DAG.
)

fetch_events = BashOperator(
    task_id="fetch_events",
    bash_command=(
        "mkdir -p /data/events && "
        "curl -o /data/events.json http://events_api:5000/events" # @NOTE Fetch and store the events from the API.
    ),
    dag=dag,
)


def _calculate_stats(input_path, output_path):
    """Calculates event statistics."""

    # @NOTE Load the events and calculate the required statistics.
    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index()

    # @NOTE Make sure the output directory exists and write results to CSV.
    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index=False)


calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={"input_path": "/data/events.json", "output_path": "/data/stats.csv"},
    dag=dag,
)

fetch_events >> calculate_stats
