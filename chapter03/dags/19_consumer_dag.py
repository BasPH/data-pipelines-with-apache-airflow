from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator

FILEPATH = "/tmp/launches.csv"

target_dataset = Dataset(FILEPATH)


def print_data():
    try:
        data = pd.read_csv(FILEPATH)
    except FileNotFoundError:
        raise Exception("No data found")

    print(f"There has been {data['count'].max()} launches")


with DAG(
    dag_id="19_report_launches_consumer",
    schedule=[target_dataset],
    start_date=datetime(2023, 12, 1),
):
    PythonOperator(
        task_id="print_data",
        python_callable=print_data,
    )
