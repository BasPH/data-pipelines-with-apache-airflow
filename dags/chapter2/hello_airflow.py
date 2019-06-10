import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id="hello_airflow",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@daily",
)

first = BashOperator(
    task_id="hello",
    bash_command="echo hello",
    dag=dag,
)

second = PythonOperator(
    task_id="airflow",
    python_callable=lambda: print("airflow"),
    dag=dag,
)

first >> second
