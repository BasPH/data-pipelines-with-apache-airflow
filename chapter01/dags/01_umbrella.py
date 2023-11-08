"""DAG demonstrating the umbrella use case with empty operators."""

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="01_umbrella",
    description="Umbrella example with EmptyOperators.",
    start_date=pendulum.today("UTC").add(days=-5),
    schedule="@daily",
):
    fetch_weather_forecast = EmptyOperator(task_id="fetch_weather_forecast")
    fetch_sales_data = EmptyOperator(task_id="fetch_sales_data")
    clean_forecast_data = EmptyOperator(task_id="clean_forecast_data")
    clean_sales_data = EmptyOperator(task_id="clean_sales_data")
    join_datasets = EmptyOperator(task_id="join_datasets")
    train_ml_model = EmptyOperator(task_id="train_ml_model")
    deploy_ml_model = EmptyOperator(task_id="deploy_ml_model")

    # Set dependencies between all tasks
    fetch_weather_forecast >> clean_forecast_data
    fetch_sales_data >> clean_sales_data
    [clean_forecast_data, clean_sales_data] >> join_datasets
    join_datasets >> train_ml_model >> deploy_ml_model
