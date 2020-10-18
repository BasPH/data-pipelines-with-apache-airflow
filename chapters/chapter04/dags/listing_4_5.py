from urllib import request

import airflow.utils.dates
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id="listing_4_5",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
)


def _get_data(execution_date, **_):
    year, month, day, hour, *_ = execution_date.timetuple()
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    output_path = "/tmp/wikipageviews.gz"
    request.urlretrieve(url, output_path)


get_data = PythonOperator(
    task_id="get_data", python_callable=_get_data, provide_context=True, dag=dag
)
