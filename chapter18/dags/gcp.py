import datetime
import logging
import os
import tempfile
from os import path

import pandas as pd
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
    BigQueryDeleteTableOperator,
)
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import (
    BigQueryToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from custom.hooks import MovielensHook

dag = DAG(
    "gcp_movie_ranking",
    start_date=datetime.datetime(year=2019, month=1, day=1),
    end_date=datetime.datetime(year=2019, month=3, day=1),
    schedule_interval="@monthly",
    default_args={"depends_on_past": True},
)


def _fetch_ratings(api_conn_id, gcp_conn_id, gcs_bucket, **context):
    year = context["execution_date"].year
    month = context["execution_date"].month

    # Fetch ratings from our API.
    logging.info(f"Fetching ratings for {year}/{month:02d}")

    api_hook = MovielensHook(conn_id=api_conn_id)
    ratings = pd.DataFrame.from_records(
        api_hook.get_ratings_for_month(year=year, month=month),
        columns=["userId", "movieId", "rating", "timestamp"],
    )

    logging.info(f"Fetched {ratings.shape[0]} rows")

    # Write ratings to temp file.
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = path.join(tmp_dir, "ratings.csv")
        ratings.to_csv(tmp_path, index=False)

        # Upload file to GCS.
        logging.info(f"Writing results to ratings/{year}/{month:02d}.csv")
        gcs_hook = GCSHook(gcp_conn_id)
        gcs_hook.upload(
            bucket_name=gcs_bucket,
            object_name=f"ratings/{year}/{month:02d}.csv",
            filename=tmp_path,
        )


fetch_ratings = PythonOperator(
    task_id="fetch_ratings",
    python_callable=_fetch_ratings,
    op_kwargs={
        "api_conn_id": "movielens",
        "gcp_conn_id": "gcp",
        "gcs_bucket": os.environ["RATINGS_BUCKET"],
    },
    dag=dag,
)


import_in_bigquery = GCSToBigQueryOperator(
    task_id="import_in_bigquery",
    bucket=os.environ["RATINGS_BUCKET"],
    source_objects=[
        "ratings/{{ execution_date.year }}/{{ execution_date.strftime('%m') }}.csv"
    ],
    source_format="CSV",
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    bigquery_conn_id="gcp",
    skip_leading_rows=1,
    schema_fields=[
        {"name": "userId", "type": "INTEGER"},
        {"name": "movieId", "type": "INTEGER"},
        {"name": "rating", "type": "FLOAT"},
        {"name": "timestamp", "type": "TIMESTAMP"},
    ],
    destination_project_dataset_table=(
        os.environ["GCP_PROJECT"]
        + ":"
        + os.environ["BIGQUERY_DATASET"]
        + "."
        + "ratings${{ ds_nodash }}"
    ),
    dag=dag,
)

query_top_ratings = BigQueryExecuteQueryOperator(
    task_id="query_top_ratings",
    destination_dataset_table=(
        os.environ["GCP_PROJECT"]
        + ":"
        + os.environ["BIGQUERY_DATASET"]
        + "."
        + "rating_results_{{ ds_nodash }}"
    ),
    sql=(
        "SELECT movieid, AVG(rating) as avg_rating, COUNT(*) as num_ratings "
        "FROM " + os.environ["BIGQUERY_DATASET"] + ".ratings "
        "WHERE DATE(timestamp) <= DATE({{ ds }}) "
        "GROUP BY movieid "
        "ORDER BY avg_rating DESC"
    ),
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
    bigquery_conn_id="gcp",
    dag=dag,
)

extract_top_ratings = BigQueryToGCSOperator(
    task_id="extract_top_ratings",
    source_project_dataset_table=(
        os.environ["GCP_PROJECT"]
        + ":"
        + os.environ["BIGQUERY_DATASET"]
        + "."
        + "rating_results_{{ ds_nodash }}"
    ),
    destination_cloud_storage_uris=[
        "gs://" + os.environ["RESULT_BUCKET"] + "/{{ ds_nodash }}.csv"
    ],
    export_format="CSV",
    bigquery_conn_id="gcp",
    dag=dag,
)

delete_result_table = BigQueryDeleteTableOperator(
    task_id="delete_result_table",
    deletion_dataset_table=(
        os.environ["GCP_PROJECT"]
        + ":"
        + os.environ["BIGQUERY_DATASET"]
        + "."
        + "rating_results_{{ ds_nodash }}"
    ),
    bigquery_conn_id="gcp",
    dag=dag,
)

fetch_ratings >> import_in_bigquery >> query_top_ratings >> extract_top_ratings >> delete_result_table
