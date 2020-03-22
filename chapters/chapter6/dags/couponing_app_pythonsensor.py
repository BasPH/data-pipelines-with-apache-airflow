from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.contrib.sensors.python_sensor import PythonSensor
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id="chapter6_couponing_app_pythonsensor",
    start_date=datetime(2019, 1, 1),
    schedule_interval="*/2 * * * *",
    concurrency=16,
    description="A batch workflow for ingesting supermarket promotions data, demonstrating the PythonSensor.",
)

create_metrics = DummyOperator(task_id="create_metrics", dag=dag)


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
        timeout=600,
        mode="reschedule",
        dag=dag,
    )
    copy = DummyOperator(task_id=f"copy_to_raw_supermarket_{supermarket_id}", dag=dag)
    process = DummyOperator(task_id=f"process_supermarket_{supermarket_id}", dag=dag)
    wait >> copy >> process >> create_metrics
