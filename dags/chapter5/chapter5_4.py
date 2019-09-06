import airflow

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator


def _pick_erp_system(**context):
    if context["execution_date"] < airflow.utils.dates.days_ago(1):
        return "fetch_sales_old"
    else:
        return "fetch_sales_new"

with DAG(
    dag_id="chapter5_4",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily",
) as dag:
    start = DummyOperator(task_id="start")

    sales_branch = BranchPythonOperator(
        task_id='pick_erp_system',
        provide_context=True,
        python_callable=_pick_erp_system,
    )

    fetch_sales_old = DummyOperator(task_id="fetch_sales_old")
    preprocess_sales_old = DummyOperator(task_id="preprocess_sales_old")

    fetch_sales_new = DummyOperator(task_id="fetch_sales_new")
    preprocess_sales_new = DummyOperator(task_id="preprocess_sales_new")

    fetch_weather = DummyOperator(task_id="fetch_weather")
    preprocess_weather = DummyOperator(task_id="preprocess_weather")

    build_dataset = DummyOperator(task_id="build_dataset", trigger_rule="none_failed")
    train_model = DummyOperator(task_id="train_model")
    notify = DummyOperator(task_id="notify")

    start >> [sales_branch, fetch_weather]
    sales_branch >> [fetch_sales_old, fetch_sales_new]
    fetch_sales_old >> preprocess_sales_old
    fetch_sales_new >> preprocess_sales_new
    fetch_weather >> preprocess_weather
    [preprocess_sales_old, preprocess_sales_new, preprocess_weather] >> build_dataset
    build_dataset >> train_model >> notify
