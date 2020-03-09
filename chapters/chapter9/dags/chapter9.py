from datetime import datetime

from airflow import DAG

from custom.postgres_to_s3_operator import PostgresToS3Operator

dag = DAG(
    dag_id="chapter9",
    start_date=datetime(2011, 10, 14),
    end_date=datetime(2019, 12, 8),
    schedule_interval=None,
)

download_from_postgres = PostgresToS3Operator(
    task_id="download_from_postgres",
    postgres_conn_id="inside_airbnb",
    query="SELECT * FROM listings WHERE download_date={{ ds }}",
    s3_conn_id="locals3",
    s3_bucket="inside_airbnb",
    s3_key="listing-{{ ds }}.csv",
    dag=dag,
)
