from pprint import pprint

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


def _print_context(**kwargs):
    pprint(kwargs)


with DAG(
    dag_id="03_print_context",
    start_date=pendulum.today("UTC").add(days=-3),
    schedule="@hourly",
):
    print_context = PythonOperator(
        task_id="print_context",
        python_callable=_print_context,
    )
