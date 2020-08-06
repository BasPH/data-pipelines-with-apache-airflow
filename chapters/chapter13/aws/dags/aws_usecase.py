import datetime as dt
import os
from os import path
import tempfile

from airflow import DAG

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
from airflow.operators.python_operator import PythonOperator

from custom.operators import GlueTriggerCrawlerOperator
from custom.ratings import fetch_ratings


with DAG(
    dag_id="chapter13_aws_usecase",
    description="DAG demonstrating some AWS-specific hooks and operators.",
    start_date=dt.datetime(year=2015, month=1, day=1),
    end_date=dt.datetime(year=2015, month=3, day=1),
    schedule_interval="@monthly",
    default_args={"depends_on_past": True},
) as dag:

    def _upload_ratings(s3_conn_id, s3_bucket, **context):
        year = context["execution_date"].year
        month = context["execution_date"].month

        # Fetch ratings from our 'API'.
        ratings = fetch_ratings(year=year, month=month)

        # Write ratings to temp file.
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = path.join(tmp_dir, "ratings.csv")
            ratings.to_csv(tmp_path, index=False)

            # Upload file to S3.
            hook = S3Hook(s3_conn_id)
            hook.load_file(
                tmp_path,
                key=f"ratings/{year}/{month}.csv",
                bucket_name=s3_bucket,
                replace=True,
            )

    upload_ratings = PythonOperator(
        task_id="fetch_ratings",
        python_callable=_upload_ratings,
        op_kwargs={
            "s3_conn_id": "my_aws_conn",
            "s3_bucket": os.environ["RATINGS_BUCKET"],
        },
        provide_context=True,
    )

    trigger_crawler = GlueTriggerCrawlerOperator(
        aws_conn_id="my_aws_conn",
        task_id="trigger_crawler",
        crawler_name=os.environ["CRAWLER_NAME"],
    )

    rank_movies = AWSAthenaOperator(
        task_id="rank_movies",
        aws_conn_id="my_aws_conn",
        database="airflow",
        query="""
            SELECT movieid, AVG(rating) as avg_rating, COUNT(*) as num_ratings
            FROM (
                SELECT movieid, rating, CAST(from_unixtime(timestamp) AS DATE) AS date
                FROM ratings
            )
            WHERE date <= DATE('{{ ds }}')
            GROUP BY movieid
            ORDER BY avg_rating DESC
        """,
        output_location=f"s3://{os.environ['RANKINGS_BUCKET']}/{{{{ds}}}}",
    )

    upload_ratings >> trigger_crawler >> rank_movies
