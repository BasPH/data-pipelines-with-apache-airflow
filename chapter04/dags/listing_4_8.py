import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


def _print_context(**context):
    start = context["execution_date"]
    end = context["next_execution_date"]
    print(f"Start: {start}, end: {end}")
    # Prints e.g.:
    # Start: 2019-07-13T14:00:00+00:00, end: 2019-07-13T15:00:00+00:00


with DAG(
    dag_id="listing_4_08",
    start_date=pendulum.today("UTC").add(days=-3),
    schedule="@daily",
):
    print_context = PythonOperator(
        task_id="print_context",
        python_callable=_print_context,
    )
