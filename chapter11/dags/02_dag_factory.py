import os

import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator


def generate_dag(dataset_name, raw_dir, processed_dir, preprocess_script):

    with DAG(
        dag_id=f"02_dag_factory_{dataset_name}",
        start_date=airflow.utils.dates.days_ago(5),
        schedule_interval="@daily",
    ) as dag:

        raw_file_path = os.path.join(raw_dir, dataset_name, "{ds_nodash}.json")
        processed_file_path = os.path.join(
            processed_dir, dataset_name, "{ds_nodash}.json"
        )

        fetch_task = BashOperator(
            task_id=f"fetch_{dataset_name}",
            bash_command=f"echo 'curl http://example.com/{dataset_name}.json > {raw_file_path}.json'",
        )

        preprocess_task = BashOperator(
            task_id=f"preprocess_{dataset_name}",
            bash_command=f"echo '{preprocess_script} {raw_file_path} {processed_file_path}'",
        )

        fetch_task >> preprocess_task

    return dag


for dataset in ["sales", "customers"]:
    globals()[f"02_dag_factory_{dataset}"] = generate_dag(
        dataset_name=dataset,
        raw_dir="/data/raw",
        processed_dir="/data/processed",
        preprocess_script=f"preprocess_{dataset}.py",
    )
