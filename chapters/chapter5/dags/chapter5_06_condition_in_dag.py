import airflow
import pendulum

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

ERP_CHANGE_DATE = airflow.utils.dates.days_ago(1)


def _pick_erp_system(**context):
    if context["execution_date"] < ERP_CHANGE_DATE:
        return "fetch_sales_old"
    else:
        return "fetch_sales_new"


def _latest_only(**context):
    now = pendulum.utcnow()
    left_window = context["dag"].following_schedule(context["execution_date"])
    right_window = context["dag"].following_schedule(left_window)

    if not left_window < now <= right_window:
        raise AirflowSkipException()


with DAG(
    dag_id="chapter5_06_condition_in_dag",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily",
) as dag:
    start = DummyOperator(task_id="start")

    pick_erp = BranchPythonOperator(
        task_id="pick_erp_system",
        provide_context=True,
        python_callable=_pick_erp_system,
    )

    fetch_sales_old = DummyOperator(task_id="fetch_sales_old")
    preprocess_sales_old = DummyOperator(task_id="preprocess_sales_old")

    fetch_sales_new = DummyOperator(task_id="fetch_sales_new")
    preprocess_sales_new = DummyOperator(task_id="preprocess_sales_new")

    join_erp = DummyOperator(task_id="join_erp_branch", trigger_rule="none_failed")

    fetch_weather = DummyOperator(task_id="fetch_weather")
    preprocess_weather = DummyOperator(task_id="preprocess_weather")

    build_dataset = DummyOperator(task_id="build_dataset")
    train_model = DummyOperator(task_id="train_model")

    if_most_recent = PythonOperator(
        task_id="latest_only", python_callable=_latest_only, provide_context=True,
    )

    notify = DummyOperator(task_id="notify")

    start >> [pick_erp, fetch_weather]
    pick_erp >> [fetch_sales_old, fetch_sales_new]
    fetch_sales_old >> preprocess_sales_old
    fetch_sales_new >> preprocess_sales_new
    [preprocess_sales_old, preprocess_sales_new] >> join_erp
    fetch_weather >> preprocess_weather
    [join_erp, preprocess_weather] >> build_dataset
    build_dataset >> train_model >> notify
    if_most_recent >> notify
