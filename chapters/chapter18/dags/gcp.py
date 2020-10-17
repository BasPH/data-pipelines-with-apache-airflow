import datetime
import os

from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_table_delete_operator import (
    BigQueryTableDeleteOperator,
)
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.models import DAG

dag = DAG(
    "gcp_movie_ranking",
    start_date=datetime.datetime(1995, 1, 1),
    schedule_interval="@monthly",
)


upload_ratings_to_gcs = FileToGoogleCloudStorageOperator(
    task_id="upload_ratings_to_gcs",
    src="/data/{{ execution_date.year }}/{{ execution_date.strftime('%m') }}.csv",
    bucket=os.environ["RATINGS_BUCKET"],
    dst="ratings/{{ execution_date.year }}/{{ execution_date.strftime('%m') }}.csv",
    dag=dag,
)


import_in_bigquery = GoogleCloudStorageToBigQueryOperator(
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

query_top_ratings = BigQueryOperator(
    task_id="query_top_ratings",
    destination_dataset_table=(
        os.environ["GCP_PROJECT"]
        + ":"
        + os.environ["BIGQUERY_DATASET"]
        + "."
        + "rating_results_{{ ds_nodash }}"
    ),
    sql="""SELECT movieid, AVG(rating) as avg_rating, COUNT(*) as num_ratings
FROM airflow.ratings
WHERE DATE(timestamp) <= DATE({{ ds }})
GROUP BY movieid
ORDER BY avg_rating DESC
""",
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
    bigquery_conn_id="gcp",
    dag=dag,
)

extract_top_ratings = BigQueryToCloudStorageOperator(
    task_id="extract_top_ratings",
    source_project_dataset_table=(
        os.environ["GCP_PROJECT"]
        + ":"
        + os.environ["BIGQUERY_DATASET"]
        + "."
        + "rating_results_{{ ds_nodash }}"
    ),
    destination_cloud_storage_uris=(
        "gs://" + os.environ["RESULT_BUCKET"] + "/{{ ds_nodash }}.csv"
    ),
    export_format="CSV",
    bigquery_conn_id="gcp",
    dag=dag,
)

delete_result_table = BigQueryTableDeleteOperator(
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

upload_ratings_to_gcs >> import_in_bigquery >> query_top_ratings >> extract_top_ratings >> delete_result_table
