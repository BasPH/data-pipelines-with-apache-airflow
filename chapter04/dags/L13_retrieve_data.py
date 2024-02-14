from urllib import request

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


def _get_data(year, month, day, hour, output_path, **_):
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    request.urlretrieve(url, output_path)


with DAG(
    dag_id="L13_retrieve_data",
    start_date=pendulum.today("UTC").add(days=-1),
    schedule="@hourly",
    max_active_runs=1,
):
    get_data = PythonOperator(
        task_id="get_data",
        python_callable=_get_data,
        op_kwargs={
            "year": "{{ data_interval_start.year }}",
            "month": "{{ data_interval_start.month }}",
            "day": "{{ data_interval_start.day }}",
            "hour": "{{ data_interval_start.hour }}",
            "output_path": "/tmp/wikipageviews.gz",
        },
    )
