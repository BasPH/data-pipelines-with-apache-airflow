import pandas as pd
import pendulum
import requests
from airflow import DAG
from airflow.datasets import Dataset
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

FILEPATH = "/data/launches.csv"
URL = "https://lldev.thespacedevs.com/2.0.0/launch"

target_dataset = Dataset(FILEPATH)


def _load_csv():
    try:
        data = pd.read_csv(FILEPATH)
    except FileNotFoundError:
        data = pd.DataFrame({"count": [0]})
    return data


def _fetch_latest_launches():
    return requests.get(URL).json()["count"]


def _save_csv(data: pd.DataFrame, new_count: int):
    pd.concat([data, pd.DataFrame({"count": [new_count]})], ignore_index=True).to_csv(FILEPATH, index=False)


def _fetch_new_launches():
    data = _load_csv()
    previous_count = data["count"].max()
    new_count = _fetch_latest_launches()

    if new_count > previous_count:
        _save_csv(data, new_count)
    else:
        raise AirflowSkipException("No new launches")


with DAG(
    dag_id="L16_fetch_launches_producer",
    schedule="* * * * * ",
    start_date=pendulum.today("UTC"),
    catchup=False,
):
    PythonOperator(
        task_id="fetch_new_launches",
        python_callable=_fetch_new_launches,
        outlets=[target_dataset],
    )
