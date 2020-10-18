import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

dag = DAG(
    dag_id="chapter_9_bash_operator_no_command",
    start_date=airflow.utils.dates.days_ago(3),
    description="This DAG is intentionally faulty to demonstrate the DAG integrity test.",
    schedule_interval=None,
)

BashOperator(task_id="this_should_fail", dag=dag)
