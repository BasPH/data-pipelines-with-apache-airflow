import pendulum
from airflow import DAG

with DAG(
    dag_id="03_download_rocket_launches",
    start_date=pendulum.today("UTC").add(days=-14),
    schedule=None,
):
    ...
