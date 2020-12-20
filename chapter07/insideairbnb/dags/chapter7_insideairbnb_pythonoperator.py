import io
from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from custom.postgres_to_s3_operator import PostgresToS3Operator
from minio import Minio

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


def _crunch_numbers():
    s3_conn = BaseHook.get_connection("locals3")
    client = Minio(
        s3_conn.extra_dejson["host"].replace("http://", ""),
        access_key=s3_conn.login,
        secret_key=s3_conn.password,
        secure=False,
    )

    # Get list of all objects
    objects = [
        obj.object_name
        for obj in client.list_objects(bucket_name="inside-airbnb", prefix="listing")
    ]
    df = pd.DataFrame()
    for obj in objects:
        response = client.get_object(bucket_name="inside-airbnb", object_name=obj)
        temp_df = pd.read_csv(
            io.BytesIO(response.read()),
            usecols=["id", "price", "download_date"],
            parse_dates=["download_date"],
        )
        df = df.append(temp_df)

    # Per id, get the price increase/decrease
    # There's probably a nicer way to do this
    min_max_per_id = (
        df.groupby(["id"])
        .agg(
            download_date_min=("download_date", "min"),
            download_date_max=("download_date", "max"),
        )
        .reset_index()
    )
    df_with_min = (
        pd.merge(
            min_max_per_id,
            df,
            how="left",
            left_on=["id", "download_date_min"],
            right_on=["id", "download_date"],
        )
        .rename(columns={"price": "oldest_price"})
        .drop("download_date", axis=1)
    )
    df_with_max = (
        pd.merge(
            df_with_min,
            df,
            how="left",
            left_on=["id", "download_date_max"],
            right_on=["id", "download_date"],
        )
        .rename(columns={"price": "latest_price"})
        .drop("download_date", axis=1)
    )

    df_with_max = df_with_max[
        df_with_max["download_date_max"] != df_with_max["download_date_min"]
    ]
    df_with_max["price_diff_per_day"] = (
        df_with_max["latest_price"] - df_with_max["oldest_price"]
    ) / ((df_with_max["download_date_max"] - df_with_max["download_date_min"]).dt.days)
    df_with_max[["price_diff_per_day"]] = df_with_max[["price_diff_per_day"]].apply(
        pd.to_numeric
    )
    biggest_increase = df_with_max.nlargest(5, "price_diff_per_day")
    biggest_decrease = df_with_max.nsmallest(5, "price_diff_per_day")

    # We found the top 5, write back the results.
    biggest_increase_json = biggest_increase.to_json(orient="records")
    print(f"Biggest increases: {biggest_increase_json}")
    biggest_increase_bytes = biggest_increase_json.encode("utf-8")
    client.put_object(
        bucket_name="inside-airbnb",
        object_name="results/biggest_increase.json",
        data=io.BytesIO(biggest_increase_bytes),
        length=len(biggest_increase_bytes),
    )

    biggest_decrease_json = biggest_decrease.to_json(orient="records")
    print(f"Biggest decreases: {biggest_decrease_json}")
    biggest_decrease_bytes = biggest_decrease_json.encode("utf-8")
    client.put_object(
        bucket_name="inside-airbnb",
        object_name="results/biggest_decrease.json",
        data=io.BytesIO(biggest_decrease_bytes),
        length=len(biggest_decrease_bytes),
    )


crunch_numbers = PythonOperator(
    task_id="crunch_numbers", python_callable=_crunch_numbers, dag=dag
)


download_from_postgres >> crunch_numbers
