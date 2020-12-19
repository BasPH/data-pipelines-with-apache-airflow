import io
import json

import airflow.utils.dates
import geopandas
import pandas as pd
import requests
from airflow.hooks.base import BaseHook
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from minio import Minio
from nyctransport.operators.pandas_operator import PandasOperator
from nyctransport.operators.s3_to_postgres import MinioPandasToPostgres
from requests.auth import HTTPBasicAuth

dag = DAG(
    dag_id="nyc_dag",
    schedule_interval="*/15 * * * *",
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,
)


def _download_citi_bike_data(ts_nodash, **_):
    citibike_conn = BaseHook.get_connection(conn_id="citibike")

    url = f"http://{citibike_conn.host}:{citibike_conn.port}/recent/minute/15"
    response = requests.get(
        url, auth=HTTPBasicAuth(citibike_conn.login, citibike_conn.password)
    )
    data = response.json()

    s3_hook = S3Hook(aws_conn_id="s3")
    s3_hook.load_string(
        string_data=json.dumps(data),
        key=f"raw/citibike/{ts_nodash}.json",
        bucket_name="datalake",
    )


download_citi_bike_data = PythonOperator(
    task_id="download_citi_bike_data", python_callable=_download_citi_bike_data, dag=dag
)


def _download_taxi_data():
    taxi_conn = BaseHook.get_connection(conn_id="taxi")
    s3_hook = S3Hook(aws_conn_id="s3")

    url = f"http://{taxi_conn.host}"
    response = requests.get(url)
    files = response.json()

    exported_files = []
    for filename in [f["name"] for f in files]:
        response = requests.get(f"{url}/{filename}")
        s3_key = f"raw/taxi/{filename}"
        try:
            s3_hook.load_string(
                string_data=response.text, key=s3_key, bucket_name="datalake"
            )
            print(f"Uploaded {s3_key} to MinIO.")
            exported_files.append(s3_key)
        except ValueError:
            print(f"File {s3_key} already exists.")

    return exported_files


download_taxi_data = PythonOperator(
    task_id="download_taxi_data", python_callable=_download_taxi_data, dag=dag
)


def get_minio_object(
    pandas_read_callable, bucket, paths, pandas_read_callable_kwargs=None
):
    s3_conn = BaseHook.get_connection(conn_id="s3")
    minio_client = Minio(
        s3_conn.extra_dejson["host"].split("://")[1],
        access_key=s3_conn.extra_dejson["aws_access_key_id"],
        secret_key=s3_conn.extra_dejson["aws_secret_access_key"],
        secure=False,
    )

    if isinstance(paths, str):
        if paths.startswith("[") and paths.endswith("]"):
            paths = eval(paths)
        else:
            paths = [paths]

    if pandas_read_callable_kwargs is None:
        pandas_read_callable_kwargs = {}

    dfs = []
    for path in paths:
        minio_object = minio_client.get_object(bucket_name=bucket, object_name=path)
        df = pandas_read_callable(minio_object, **pandas_read_callable_kwargs)
        dfs.append(df)
    return pd.concat(dfs)


def transform_citi_bike_data(df):
    # Map citi bike lat,lon coordinates to taxi zone ids
    taxi_zones = geopandas.read_file(
        "https://s3.amazonaws.com/nyc-tlc/misc/taxi_zones.zip"
    ).to_crs("EPSG:4326")
    start_gdf = geopandas.GeoDataFrame(
        df,
        crs="EPSG:4326",
        geometry=geopandas.points_from_xy(
            df["start_station_longitude"], df["start_station_latitude"]
        ),
    )
    end_gdf = geopandas.GeoDataFrame(
        df,
        crs="EPSG:4326",
        geometry=geopandas.points_from_xy(
            df["end_station_longitude"], df["end_station_latitude"]
        ),
    )
    df_with_zones = geopandas.sjoin(
        start_gdf, taxi_zones, how="left", op="within"
    ).rename(columns={"LocationID": "start_location_id"})
    end_zones = geopandas.sjoin(end_gdf, taxi_zones, how="left", op="within")
    df_with_zones["end_location_id"] = end_zones["LocationID"]
    return df_with_zones[
        [
            "tripduration",
            "starttime",
            "start_location_id",
            "stoptime",
            "end_location_id",
        ]
    ]


def write_minio_object(
    df, pandas_write_callable, bucket, path, pandas_write_callable_kwargs=None
):
    s3_conn = BaseHook.get_connection(conn_id="s3")
    minio_client = Minio(
        s3_conn.extra_dejson["host"].split("://")[1],
        access_key=s3_conn.extra_dejson["aws_access_key_id"],
        secret_key=s3_conn.extra_dejson["aws_secret_access_key"],
        secure=False,
    )
    bytes_buffer = io.BytesIO()
    pandas_write_method = getattr(df, pandas_write_callable.__name__)
    pandas_write_method(bytes_buffer, **pandas_write_callable_kwargs)
    nbytes = bytes_buffer.tell()
    bytes_buffer.seek(0)
    minio_client.put_object(
        bucket_name=bucket, object_name=path, length=nbytes, data=bytes_buffer
    )


process_citi_bike_data = PandasOperator(
    task_id="process_citi_bike_data",
    input_callable=get_minio_object,
    input_callable_kwargs={
        "pandas_read_callable": pd.read_json,
        "bucket": "datalake",
        "paths": "raw/citibike/{{ ts_nodash }}.json",
    },
    transform_callable=transform_citi_bike_data,
    output_callable=write_minio_object,
    output_callable_kwargs={
        "bucket": "datalake",
        "path": "processed/citibike/{{ ts_nodash }}.parquet",
        "pandas_write_callable": pd.DataFrame.to_parquet,
        "pandas_write_callable_kwargs": {"engine": "auto"},
    },
    dag=dag,
)


def transform_taxi_data(df):
    df[["pickup_datetime", "dropoff_datetime"]] = df[
        ["pickup_datetime", "dropoff_datetime"]
    ].apply(pd.to_datetime)
    df["tripduration"] = (
        (df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds().astype(int)
    )
    df = df.rename(
        columns={
            "pickup_datetime": "starttime",
            "pickup_locationid": "start_location_id",
            "dropoff_datetime": "stoptime",
            "dropoff_locationid": "end_location_id",
        }
    ).drop(columns=["trip_distance"])
    return df


process_taxi_data = PandasOperator(
    task_id="process_taxi_data",
    input_callable=get_minio_object,
    input_callable_kwargs={
        "pandas_read_callable": pd.read_csv,
        "bucket": "datalake",
        "paths": "{{ ti.xcom_pull(task_ids='download_taxi_data') }}",
    },
    transform_callable=transform_taxi_data,
    output_callable=write_minio_object,
    output_callable_kwargs={
        "bucket": "datalake",
        "path": "processed/taxi/{{ ts_nodash }}.parquet",
        "pandas_write_callable": pd.DataFrame.to_parquet,
        "pandas_write_callable_kwargs": {"engine": "auto"},
    },
    dag=dag,
)

taxi_to_db = MinioPandasToPostgres(
    task_id="taxi_to_db",
    dag=dag,
    minio_conn_id="s3",
    minio_bucket="datalake",
    minio_key="processed/taxi/{{ ts_nodash }}.parquet",
    pandas_read_callable=pd.read_parquet,
    postgres_conn_id="result_db",
    postgres_table="taxi_rides",
    pre_read_transform=lambda x: io.BytesIO(x.data),
)

citi_bike_to_db = MinioPandasToPostgres(
    task_id="citi_bike_to_db",
    dag=dag,
    minio_conn_id="s3",
    minio_bucket="datalake",
    minio_key="processed/citibike/{{ ts_nodash }}.parquet",
    pandas_read_callable=pd.read_parquet,
    postgres_conn_id="result_db",
    postgres_table="citi_bike_rides",
    pre_read_transform=lambda x: io.BytesIO(x.data),
)

download_citi_bike_data >> process_citi_bike_data >> citi_bike_to_db
download_taxi_data >> process_taxi_data >> taxi_to_db
