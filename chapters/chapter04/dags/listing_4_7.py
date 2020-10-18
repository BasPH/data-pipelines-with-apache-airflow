from urllib import request

import airflow.utils.dates
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id="listing_4_7",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily",
)


def _print_context(**context):
    print(context)


print_context = PythonOperator(
    task_id="print_context",
    python_callable=_print_context,
    provide_context=True,
    dag=dag,
)
