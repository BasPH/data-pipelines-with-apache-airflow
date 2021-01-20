import airflow.utils.dates
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor

dag = DAG(
    dag_id="figure_6_08",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="0 16 * * *",
    description="Create a file /data/supermarket1/data.csv, and behold a sensor deadlock.",
)

create_metrics = DummyOperator(task_id="create_metrics", dag=dag)
for supermarket_id in [1, 2, 3, 4]:
    copy = FileSensor(
        task_id=f"copy_to_raw_supermarket_{supermarket_id}",
        filepath=f"/data/supermarket{supermarket_id}/data.csv",
        dag=dag,
    )
    process = DummyOperator(task_id=f"process_supermarket_{supermarket_id}", dag=dag)
    copy >> process >> create_metrics
