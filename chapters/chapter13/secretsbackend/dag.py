import airflow.utils.dates
from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

dag = DAG(
    dag_id="secretsbackend_with_vault",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval=None,
)

call_api = SimpleHttpOperator(
    task_id="call_api",
    http_conn_id="secure_api",
    method="GET",
    endpoint="",
    log_response=True,
    dag=dag,
)
