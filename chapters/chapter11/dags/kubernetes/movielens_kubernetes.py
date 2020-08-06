import os

from airflow import DAG, utils as airflow_utils
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount

with DAG(
    dag_id="chapter11_movielens_kubernetes",
    description="Fetches ratings from the Movielens API using kubernetes.",
    start_date=airflow_utils.dates.days_ago(3),
    schedule_interval="@daily",
) as dag:

    volume_mount = VolumeMount(
        "data-volume", mount_path="/data", sub_path=None, read_only=False
    )

    volume_config = {"persistentVolumeClaim": {"claimName": "data-volume"}}
    volume = Volume(name="data-volume", configs=volume_config)

    fetch_ratings = KubernetesPodOperator(
        task_id="fetch_ratings",
        image="airflowbook/movielens-fetch",
        cmds=["fetch_ratings.py"],
        arguments=[
            "--start_date",
            "{{ds}}",
            "--end_date",
            "{{next_ds}}",
            "--output_path",
            "/data/ratings/{{ds}}.json",
            "--user",
            os.environ["MOVIELENS_USER"],
            "--password",
            os.environ["MOVIELENS_PASSWORD"],
            "--host",
            os.environ["MOVIELENS_HOST"],
        ],
        namespace="airflow",
        name="fetch-ratings",
        cluster_context="docker-desktop",
        volumes=[volume],
        volume_mounts=[volume_mount],
    )

    rank_movies = KubernetesPodOperator(
        task_id="rank_movies",
        image="airflowbook/movielens-rank",
        cmds=["rank_movies.py"],
        arguments=[
            "--input_path",
            "/data/ratings/{{ds}}.json",
            "--output_path",
            "/data/rankings/{{ds}}.csv",
        ],
        namespace="airflow",
        name="rank-movies",
        cluster_context="docker-desktop",
        volumes=[volume],
        volume_mounts=[volume_mount],
    )

    fetch_ratings >> rank_movies
