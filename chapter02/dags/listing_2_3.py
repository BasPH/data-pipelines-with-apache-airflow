import airflow
from airflow import DAG

dag = DAG(
    dag_id="listing_2_03",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None,
)
