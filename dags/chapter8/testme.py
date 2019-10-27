import airflow.utils.dates
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(dag_id="testme", start_date=airflow.utils.dates.days_ago(3), schedule_interval=None)

task1 = DummyOperator(task_id="task1", dag=dag)
