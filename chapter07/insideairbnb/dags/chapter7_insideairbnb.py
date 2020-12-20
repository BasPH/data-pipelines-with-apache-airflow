from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from custom.postgres_to_s3_operator import PostgresToS3Operator

dag = DAG(
    dag_id="chapter7_insideairbnb",
    start_date=datetime(2015, 4, 5),
    end_date=datetime(2019, 12, 7),
    schedule_interval="@monthly",
)

download_from_postgres = PostgresToS3Operator(
    task_id="download_from_postgres",
    postgres_conn_id="inside_airbnb",
    query="SELECT * FROM listings WHERE download_date BETWEEN '{{ prev_ds }}' AND '{{ ds }}'",
    s3_conn_id="locals3",
    s3_bucket="inside-airbnb",
    s3_key="listing-{{ ds }}.csv",
    dag=dag,
)

crunch_numbers = DockerOperator(
    task_id="crunch_numbers",
    image="numbercruncher",
    api_version="auto",
    auto_remove=True,
    docker_url="unix://var/run/docker.sock",
    network_mode="host",
    environment={
        "S3_ENDPOINT": "localhost:9000",
        "S3_ACCESS_KEY": "secretaccess",
        "S3_SECRET_KEY": "secretkey",
    },
    dag=dag,
)

download_from_postgres >> crunch_numbers
