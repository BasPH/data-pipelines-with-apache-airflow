import pandas as pd
import pendulum
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator

FILEPATH = "/data/launches.csv"

target_dataset = Dataset(FILEPATH)


def _print_data():
    try:
        data = pd.read_csv(FILEPATH)
    except FileNotFoundError:
        raise Exception("No data found")

    print(f"There has been {data['count'].max()} launches")


with DAG(
    dag_id="L19_report_launches_consumer",
    schedule=[target_dataset],
    start_date=pendulum.today("UTC"),
):
    PythonOperator(
        task_id="print_data",
        python_callable=_print_data,
    )
