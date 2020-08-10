import airflow.utils.dates
from airflow.models import DAG


def send_error():
    print("ERROR!")


dag = DAG(
    dag_id="chapter12_dag_failure_callback",
    on_failure_callback=send_error,
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(3),
)
