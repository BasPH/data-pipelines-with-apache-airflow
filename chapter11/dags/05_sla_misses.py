from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


def sla_miss_callback(context):
    send_slack_message("Missed SLA!")


def send_slack_message(message):
    pass


default_args = {"sla": timedelta(seconds=10)}

with DAG(
    dag_id="05_sla_misses",
    start_date=pendulum.today("UTC").add(days=-2),
    schedule_interval="@daily",
    default_args=default_args,
    # email=['anonymous@example.com'],
    sla_miss_callback=sla_miss_callback,
) as dag:
    sleep_task = BashOperator(task_id="sleep", bash_command="sleep 30")
