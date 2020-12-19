import uuid

import airflow

from airflow import DAG
from airflow.decorators import task


with DAG(
    dag_id="12_taskflow",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily",
) as dag:

    @task
    def train_model():
        model_id = str(uuid.uuid4())
        return model_id

    @task
    def deploy_model(model_id: str):
        print(f"Deploying model {model_id}")

    model_id = train_model()
    deploy_model(model_id)
