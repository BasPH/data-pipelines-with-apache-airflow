from pathlib import Path

import airflow.utils.dates
from airflow import DAG
from airflow.contrib.sensors.python_sensor import PythonSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator

dag1 = DAG(
    dag_id="chapter6_ingest_supermarket_data_triggerdagrunoperator",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 16 * * *",
)
dag2 = DAG(
    dag_id="chapter6_ingest_supermarket_data_triggerdagrunoperator_target",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval=None,
)


def _wait_for_supermarket(supermarket_id_):
    supermarket_path = Path("/data/" + supermarket_id_)
    data_files = supermarket_path.glob("data-*.csv")
    success_file = supermarket_path / "_SUCCESS"
    return data_files and success_file.exists()


for supermarket_id in range(1, 5):
    wait = PythonSensor(
        task_id=f"wait_for_supermarket_{supermarket_id}",
        python_callable=_wait_for_supermarket,
        op_kwargs={"supermarket_id_": f"supermarket{supermarket_id}"},
        dag=dag1,
    )
    copy = DummyOperator(task_id=f"copy_to_raw_supermarket_{supermarket_id}", dag=dag1)
    process = DummyOperator(task_id=f"process_supermarket_{supermarket_id}", dag=dag1)
    trigger_create_metrics_dag = TriggerDagRunOperator(
        task_id=f"trigger_create_metrics_dag_supermarket_{supermarket_id}",
        trigger_dag_id="create_metrics",
        dag=dag1,
    )
    wait >> copy >> process >> trigger_create_metrics_dag

compute_differences = DummyOperator(task_id=f"compute_differences", dag=dag2)
update_dashboard = DummyOperator(task_id=f"update_dashboard", dag=dag2)
notify_new_data = DummyOperator(task_id=f"notify_new_data", dag=dag2)
compute_differences >> update_dashboard
