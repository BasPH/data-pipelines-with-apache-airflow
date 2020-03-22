from airflow import DAG, utils as airflow_utils

from custom.operators import MovielensFetchRatingsOperator
from custom.sensors import MovielensRatingsSensor

with DAG(
    dag_id="chapter7_movielens_sensor",
    description="Fetches ratings from the Movielens API, with a custom sensor.",
    start_date=airflow_utils.dates.days_ago(7),
    schedule_interval="@daily",
) as dag:
    wait_for_ratings = MovielensRatingsSensor(
        task_id="wait_for_ratings",
        conn_id="movielens",
        start_date="{{ds}}",
        end_date="{{next_ds}}",
    )

    fetch_ratings = MovielensFetchRatingsOperator(
        task_id="fetch_ratings",
        conn_id="movielens",
        start_date="{{ds}}",
        end_date="{{next_ds}}",
        output_path="/data/custom_sensor/{{ds}}.json",
    )

    wait_for_ratings >> fetch_ratings
