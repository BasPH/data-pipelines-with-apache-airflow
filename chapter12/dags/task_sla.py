import datetime

import airflow.utils.dates
from airflow.models import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="chapter12_task_sla",
    default_args={"email": "bob@work.com"},
    schedule_interval=datetime.timedelta(hours=12),
    start_date=airflow.utils.dates.days_ago(3),
)

sleeptask = BashOperator(
    task_id="sleeptask",
    bash_command="sleep 5",
    sla=datetime.timedelta(seconds=1),
    dag=dag,
)
