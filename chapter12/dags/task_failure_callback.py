import airflow.utils.dates
from airflow.models import DAG
from airflow.operators.bash import BashOperator


def send_error():
    print("ERROR!")


dag = DAG(
    dag_id="chapter12_task_failure_callback",
    default_args={"on_failure_callback": send_error},
    on_failure_callback=send_error,
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(3),
)

failing_task = BashOperator(task_id="failing_task", bash_command="exit 1", dag=dag)
