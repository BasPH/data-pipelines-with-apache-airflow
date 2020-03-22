import airflow

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def _fetch_sales(**context):
    if context["execution_date"] > airflow.utils.dates.days_ago(2):
        raise Exception("Something when wrong")


with DAG(
    dag_id="chapter5_07_trigger_rules",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily",
) as dag:
    start = DummyOperator(task_id="start")

    fetch_sales = PythonOperator(
        task_id="fetch_sales", python_callable=_fetch_sales, provide_context=True
    )
    preprocess_sales = DummyOperator(task_id="preprocess_sales")

    fetch_weather = DummyOperator(task_id="fetch_weather")
    preprocess_weather = DummyOperator(task_id="preprocess_weather")

    build_dataset = DummyOperator(task_id="build_dataset")
    train_model = DummyOperator(task_id="train_model")
    notify = DummyOperator(task_id="notify")

    start >> [fetch_sales, fetch_weather]
    fetch_sales >> preprocess_sales
    fetch_weather >> preprocess_weather
    [preprocess_sales, preprocess_weather] >> build_dataset
    build_dataset >> train_model >> notify
