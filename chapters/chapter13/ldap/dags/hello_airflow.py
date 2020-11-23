import airflow.utils.dates
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="hello_airflow",
    start_date=airflow.utils.dates.days_ago(3),
    max_active_runs=1,
    schedule_interval="@daily",
)

print_hello = BashOperator(task_id="print_hello", bash_command="echo 'hello'", dag=dag)
print_airflow = PythonOperator(
    task_id="print_airflow", python_callable=lambda: print("airflow"), dag=dag
)

print_hello >> print_airflow
