import pendulum
from airflow import DAG
from airflow.operators.dummy import DummyOperator

dag = DAG(
    dag_id="chapter8_duplicate_task_ids",
    start_date=pendulum.today("UTC").add(days=-3),
    schedule_interval=None,
)

t1 = DummyOperator(task_id="task", dag=dag)
for i in range(5):
    DummyOperator(task_id="task", dag=dag) >> t1
