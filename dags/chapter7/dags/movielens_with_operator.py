from airflow import DAG, utils as airflow_utils
from airflow.operators.python_operator import PythonOperator

from custom.operators import MovielensFetchRatingsOperator


with DAG(
    dag_id="chapter7_movielens_operator",
    description="Fetches ratings from a Movielens API.",
    start_date=airflow_utils.dates.days_ago(7),
    schedule_interval="@daily",
) as dag:
    MovielensFetchRatingsOperator(
        task_id="fetch_ratings",
        conn_id="movielens",
        start_date="{{ds}}",
        end_date="{{next_ds}}",
        output_path="/tmp/ratings/operator/{{ds}}.json"
    )
