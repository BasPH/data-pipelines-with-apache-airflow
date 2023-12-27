from datetime import datetime
import pandas as pd
import requests

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

FILEPATH = "/tmp/launches.csv"
URL = "https://lldev.thespacedevs.com/2.0.0/launch"

target_dataset = Dataset(FILEPATH)

def load_csv():
    try:
        data = pd.read_csv(FILEPATH)
    except FileNotFoundError:
        data = pd.DataFrame({ "count": [0]})
    return data

def fetch_latest_launches():
    return requests.get(URL).json()["count"]

def save_csv(data:pd.DataFrame,new_count:int):
    pd.concat(
        [data, pd.DataFrame({"count": [new_count]})], 
        ignore_index=True
    ).to_csv(FILEPATH, index=False)

def fetch_new_launches():
    data = load_csv()
    previous_count = data["count"].max()
    new_count = fetch_latest_launches()

    if new_count > previous_count:
        save_csv(data, new_count)
    else:
       raise AirflowSkipException("No new launches")

with DAG(
    dag_id="16_fetch_launches_producer", 
    schedule="* * * * * ",
    start_date=datetime.now(),
    catchup=False,
):
    PythonOperator(
        task_id="fetch_new_launches",
        python_callable= fetch_new_launches,
        outlets=[target_dataset],
    )
