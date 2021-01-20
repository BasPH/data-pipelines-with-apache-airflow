import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="listing_6_08",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval=None,
)


def print_conf(**context):
    print(context["dag_run"].conf)


copy_to_raw = PythonOperator(task_id="copy_to_raw", python_callable=print_conf, dag=dag)
process = PythonOperator(task_id="process", python_callable=print_conf, dag=dag)
create_metrics = PythonOperator(
    task_id="create_metrics", python_callable=print_conf, dag=dag
)
copy_to_raw >> process >> create_metrics
