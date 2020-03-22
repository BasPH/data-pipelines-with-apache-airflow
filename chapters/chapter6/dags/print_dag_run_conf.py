import airflow.utils.dates
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id="chapter6_print_dag_run_conf",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval=None,
)


def print_conf(**context):
    print(context["dag_run"].conf)


copy_to_raw = PythonOperator(
    task_id="copy_to_raw", python_callable=print_conf, provide_context=True, dag=dag
)
process = PythonOperator(
    task_id="process", python_callable=print_conf, provide_context=True, dag=dag
)
create_metrics = PythonOperator(
    task_id="create_metrics", python_callable=print_conf, provide_context=True, dag=dag
)
copy_to_raw >> process >> create_metrics
