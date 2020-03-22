"""DAG demonstrating structure between tasks with dummy nodes and dependencies."""

import airflow.utils.dates
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id="chapter1_01_figure1",
    description="DAG demonstrating structure between tasks with dummy nodes and dependencies.",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval=None,
)

tasks = {i: DummyOperator(task_id=f"task{i}", dag=dag) for i in range(1, 10)}

tasks[1] >> [tasks[2], tasks[3]]
tasks[2] >> tasks[4] >> [tasks[5], tasks[7]] >> tasks[9]
tasks[3] >> tasks[6] >> tasks[7]
tasks[5] >> tasks[7] >> tasks[8] >> tasks[9]
