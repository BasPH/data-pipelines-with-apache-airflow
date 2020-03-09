from airflow import DAG, utils as airflow_utils

from custom.operators import MovielensFetchRatingsOperator


with DAG(
    dag_id="chapter7_movielens_custom_operator",
    description="Fetches ratings from the Movielens API using a custom operator.",
    start_date=airflow_utils.dates.days_ago(7),
    schedule_interval="@daily",
) as dag:
    MovielensFetchRatingsOperator(
        task_id="fetch_ratings",
        conn_id="movielens",
        start_date="{{ds}}",
        end_date="{{next_ds}}",
        output_path="/data/custom_operator/{{ds}}.json",
    )
