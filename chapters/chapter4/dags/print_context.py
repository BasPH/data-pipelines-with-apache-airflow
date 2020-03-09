import datetime
from pprint import pprint

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id="chapter4_print_context",
    start_date=datetime.datetime(2018, 12, 10),
    schedule_interval="@daily",
)


def _print_context(**context):
    pprint(context)


# Prints e.g.:
# Start: 2019-07-13T14:00:00+00:00, end: 2019-07-13T15:00:00+00:00


print_context = PythonOperator(
    task_id="print_context",
    python_callable=_print_context,
    provide_context=True,
    dag=dag,
)
