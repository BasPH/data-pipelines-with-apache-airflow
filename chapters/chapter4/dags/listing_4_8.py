import airflow.utils.dates
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id="chapter4_listing_4_8", start_date=airflow.utils.dates.days_ago(3), schedule_interval="@daily"
)


def _print_context(**context):
    start = context["execution_date"]
    end = context["next_execution_date"]
    print(f"Start: {start}, end: {end}")


print_context = PythonOperator(
    task_id="print_context", python_callable=_print_context, provide_context=True, dag=dag
)
