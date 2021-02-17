import airflow.utils.dates
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id="chapter_9_dag_cycle",
    start_date=airflow.utils.dates.days_ago(3),
    description="This DAG is intentionally faulty to demonstrate the DAG integrity test.",
    schedule_interval=None,
)

t1 = DummyOperator(task_id="t1", dag=dag)
t2 = DummyOperator(task_id="t2", dag=dag)
t3 = DummyOperator(task_id="t3", dag=dag)

t1 >> t2 >> t3 >> t1
