import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


def _print_context(**kwargs):
    print(kwargs)



with DAG(
    dag_id="listing_4_03",
    start_date=pendulum.today("UTC").add(days=-3),
    schedule_interval="@hourly"
):


    print_context = PythonOperator(
        task_id="print_context", python_callable=_print_context, dag=dag
    )
