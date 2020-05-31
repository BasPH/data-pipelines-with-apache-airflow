import os

from airflow import DAG, utils as airflow_utils
from airflow.operators.docker_operator import DockerOperator

with DAG(
    dag_id="chapter11_movielens_docker",
    description="Fetches ratings from the Movielens API using Docker.",
    start_date=airflow_utils.dates.days_ago(3),
    schedule_interval="@daily",
) as dag:

    fetch_ratings = DockerOperator(
        task_id="fetch_ratings",
        image="airflowbook/movielens-fetch",
        command=[
            "fetch-ratings",
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
        network_mode="chapter11_airflow",
        # Note: this host path is on the HOST, not in the Airflow docker container.
        volumes=["/tmp/airflow/data:/data"],
    )

    rank_movies = DockerOperator(
        task_id="rank_movies",
        image="airflowbook/movielens-rank",
        command=[
            "rank-movies",
            "--input_path",
            "/data/ratings/{{ds}}.json",
            "--output_path",
            "/data/rankings/{{ds}}.csv",
        ],
        volumes=["/tmp/airflow/data:/data"],
    )

    fetch_ratings >> rank_movies
