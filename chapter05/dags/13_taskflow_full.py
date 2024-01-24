import uuid

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="13_taskflow_full",
    start_date=pendulum.today("UTC").add(days=-3),
    schedule="@daily",
):
    start = EmptyOperator(task_id="start")

    fetch_sales = EmptyOperator(task_id="fetch_sales")
    clean_sales = EmptyOperator(task_id="clean_sales")

    fetch_weather = EmptyOperator(task_id="fetch_weather")
    clean_weather = EmptyOperator(task_id="clean_weather")

    join_datasets = EmptyOperator(task_id="join_datasets")

    start >> [fetch_sales, fetch_weather]
    fetch_sales >> clean_sales
    fetch_weather >> clean_weather
    [clean_sales, clean_weather] >> join_datasets

    @task
    def train_model():
        model_id = str(uuid.uuid4())
        return model_id

    @task
    def deploy_model(model_id: str):
        print(f"Deploying model {model_id}")

    model_id = train_model()
    deploy_model(model_id)

    join_datasets >> model_id
