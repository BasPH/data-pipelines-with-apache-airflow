"""DAG demonstrating structure between tasks with dummy nodes and dependencies."""

import airflow.utils.dates
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id="chapter1_02_umbrella_predictions",
    description="Umbrella example with DummyOperators.",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval=None,
)

download_weather_predictions = DummyOperator(
    task_id="download_weather_predictions", dag=dag
)
download_sales_data = DummyOperator(task_id="download_sales_data", dag=dag)
clean_weather_predictions = DummyOperator(task_id="clean_weather_predictions", dag=dag)
clean_sales_data = DummyOperator(task_id="clean_sales_data", dag=dag)
join_datasets = DummyOperator(task_id="join_datasets", dag=dag)
send_report = DummyOperator(task_id="send_report", dag=dag)
train_ml_model = DummyOperator(task_id="train_ml_model", dag=dag)
deploy_ml_model = DummyOperator(task_id="deploy_ml_model", dag=dag)

# Set dependencies between all tasks
download_weather_predictions >> clean_weather_predictions
download_sales_data >> clean_sales_data
[clean_weather_predictions, clean_sales_data] >> join_datasets
join_datasets >> train_ml_model >> deploy_ml_model
join_datasets >> send_report
