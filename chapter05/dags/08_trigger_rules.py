import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def _fetch_sales(**context):
    if context["data_interval_start"] > pendulum.today("UTC").add(days=-2):
        raise Exception("Something when wrong")


with DAG(
    dag_id="08_trigger_rules",
    start_date=pendulum.today("UTC").add(days=-3),
    schedule="@daily",
):
    start = EmptyOperator(task_id="start")

    fetch_sales = PythonOperator(task_id="fetch_sales", python_callable=_fetch_sales)
    clean_sales = EmptyOperator(task_id="clean_sales")

    fetch_weather = EmptyOperator(task_id="fetch_weather")
    clean_weather = EmptyOperator(task_id="clean_weather")

    join_datasets = EmptyOperator(task_id="join_datasets")
    train_model = EmptyOperator(task_id="train_model")
    deploy_model = EmptyOperator(task_id="deploy_model")

    start >> [fetch_sales, fetch_weather]
    fetch_sales >> clean_sales
    fetch_weather >> clean_weather
    [clean_sales, clean_weather] >> join_datasets
    join_datasets >> train_model >> deploy_model
