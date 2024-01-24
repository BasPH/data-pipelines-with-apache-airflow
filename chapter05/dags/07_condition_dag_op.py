import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.python import BranchPythonOperator

ERP_CHANGE_DATE = pendulum.today("UTC").add(days=-1)


def _pick_erp_system(**context):
    if context["data_interval_start"] < ERP_CHANGE_DATE:
        return "fetch_sales_old"
    else:
        return "fetch_sales_new"


with DAG(
    dag_id="07_condition_dag_op",
    start_date=pendulum.today("UTC").add(days=-3),
    schedule="@daily",
):
    start = EmptyOperator(task_id="start")

    pick_erp = BranchPythonOperator(task_id="pick_erp_system", python_callable=_pick_erp_system)

    fetch_sales_old = EmptyOperator(task_id="fetch_sales_old")
    clean_sales_old = EmptyOperator(task_id="clean_sales_old")

    fetch_sales_new = EmptyOperator(task_id="fetch_sales_new")
    clean_sales_new = EmptyOperator(task_id="clean_sales_new")

    join_erp = EmptyOperator(task_id="join_erp_branch", trigger_rule="none_failed")

    fetch_weather = EmptyOperator(task_id="fetch_weather")
    clean_weather = EmptyOperator(task_id="clean_weather")

    join_datasets = EmptyOperator(task_id="join_datasets")
    train_model = EmptyOperator(task_id="train_model")

    latest_only = LatestOnlyOperator(task_id="latest_only")

    deploy_model = EmptyOperator(task_id="deploy_model")

    start >> [pick_erp, fetch_weather]
    pick_erp >> [fetch_sales_old, fetch_sales_new]
    fetch_sales_old >> clean_sales_old
    fetch_sales_new >> clean_sales_new
    [clean_sales_old, clean_sales_new] >> join_erp
    fetch_weather >> clean_weather
    [join_erp, clean_weather] >> join_datasets
    join_datasets >> train_model >> deploy_model
    latest_only >> deploy_model
