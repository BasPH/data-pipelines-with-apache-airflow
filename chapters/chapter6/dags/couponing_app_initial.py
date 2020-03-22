import airflow.utils.dates
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id="chapter6_couponing_app_initial",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@daily",
    description="A batch workflow for ingesting supermarket promotions data.",
    default_args={"depends_on_past": True},
)

copy_to_raw_supermarket_1 = DummyOperator(task_id="copy_to_raw_supermarket_1", dag=dag)
copy_to_raw_supermarket_2 = DummyOperator(task_id="copy_to_raw_supermarket_2", dag=dag)
copy_to_raw_supermarket_3 = DummyOperator(task_id="copy_to_raw_supermarket_3", dag=dag)
copy_to_raw_supermarket_4 = DummyOperator(task_id="copy_to_raw_supermarket_4", dag=dag)

process_supermarket_1 = DummyOperator(task_id="process_supermarket_1", dag=dag)
process_supermarket_2 = DummyOperator(task_id="process_supermarket_2", dag=dag)
process_supermarket_3 = DummyOperator(task_id="process_supermarket_3", dag=dag)
process_supermarket_4 = DummyOperator(task_id="process_supermarket_4", dag=dag)

create_metrics = DummyOperator(task_id="create_metrics", dag=dag)

copy_to_raw_supermarket_1 >> process_supermarket_1
copy_to_raw_supermarket_2 >> process_supermarket_2
copy_to_raw_supermarket_3 >> process_supermarket_3
copy_to_raw_supermarket_4 >> process_supermarket_4
[
    process_supermarket_1,
    process_supermarket_2,
    process_supermarket_3,
    process_supermarket_4,
] >> create_metrics
