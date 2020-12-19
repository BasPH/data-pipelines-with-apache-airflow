from pathlib import Path

import airflow.utils.dates
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.python import PythonSensor

dag = DAG(
    dag_id="figure_6_12",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="0 16 * * *",
    description="A batch workflow for ingesting supermarket promotions data.",
    default_args={"depends_on_past": True},
)


def _wait_for_supermarket(supermarket_id_):
    supermarket_path = Path("/data/" + supermarket_id_)
    data_files = supermarket_path.glob("data-*.csv")
    success_file = supermarket_path / "_SUCCESS"
    return data_files and success_file.exists()


for supermarket_id in [1, 2, 3, 4]:
    wait = PythonSensor(
        task_id=f"wait_for_supermarket_{supermarket_id}",
        python_callable=_wait_for_supermarket,
        op_kwargs={"supermarket_id": f"supermarket{supermarket_id}"},
        dag=dag,
    )
    copy = DummyOperator(task_id=f"copy_to_raw_supermarket_{supermarket_id}", dag=dag)
    process = DummyOperator(task_id=f"process_supermarket_{supermarket_id}", dag=dag)
    generate_metrics = DummyOperator(
        task_id=f"generate_metrics_supermarket_{supermarket_id}", dag=dag
    )
    compute_differences = DummyOperator(
        task_id=f"compute_differences_supermarket_{supermarket_id}", dag=dag
    )
    update_dashboard = DummyOperator(
        task_id=f"update_dashboard_supermarket_{supermarket_id}", dag=dag
    )
    notify_new_data = DummyOperator(
        task_id=f"notify_new_data_supermarket_{supermarket_id}", dag=dag
    )

    wait >> copy >> process >> generate_metrics >> [
        compute_differences,
        notify_new_data,
    ]
    compute_differences >> update_dashboard
