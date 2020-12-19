import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="listing_4_03",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily",
)


def _print_context(**kwargs):
    print(kwargs)
    print({k: type(v) for k,v in kwargs.items()})

    print(kwargs["prev_execution_date_success"])
    print(type(kwargs["prev_execution_date_success"]))

    print(kwargs["prev_start_date_success"])
    print(type(kwargs["prev_start_date_success"]).__module__)


print_context = PythonOperator(
    task_id="print_context",
    python_callable=_print_context,
    dag=dag,
)
