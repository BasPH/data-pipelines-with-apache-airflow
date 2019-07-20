import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id="print_context",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@hourly",
)


def _print_context(**context):
    start = context["execution_date"]
    end = context["next_execution_date"]
    print(f"Start: {start}, end: {end}")

# Prints e.g.:
# Start: 2019-07-13T14:00:00+00:00, end: 2019-07-13T15:00:00+00:00


print_context = PythonOperator(
    task_id="print_context", python_callable=_print_context, provide_context=True, dag=dag
)
