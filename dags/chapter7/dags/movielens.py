import json

from airflow import DAG, utils as airflow_utils
from airflow.operators.python_operator import PythonOperator

from custom.hooks import MovielensHook


with DAG(
    dag_id="chapter7_movielens_hook",
    description="Fetches ratings from a Movielens API.",
    start_date=airflow_utils.dates.days_ago(7),
    schedule_interval="@daily",
) as dag:

    def _fetch_ratings(templates_dict, ds, next_ds, **_):
        output_path = templates_dict["output_path"]

        print("Fetching ratings from movielens")
        hook = MovielensHook(conn_id="movielens")
        ratings = list(
            hook.get_ratings(start_date=ds, end_date=next_ds, batch_size=1000)
        )
        print(f"Fetched {len(ratings)} ratings")

        print(f"Writing ratings to {output_path}")
        with open(output_path, "w") as file_:
            json.dump(ratings, fp=file_)

    PythonOperator(
        task_id="fetch_ratings",
        python_callable=_fetch_ratings,
        templates_dict={"output_path": "/tmp/ratings-{{ds}}.json"},
        provide_context=True,
    )
