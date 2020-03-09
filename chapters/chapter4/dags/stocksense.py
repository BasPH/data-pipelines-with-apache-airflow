# """
# Documentation of pageview format: https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Traffic/Pageviews
# """

import pathlib
from urllib import request

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id="chapter4_stocksense",
    start_date=airflow.utils.dates.days_ago(30),
    schedule_interval="@hourly",
    template_searchpath="/tmp",
)


def _get_data(year, month, day, hour, ts_nodash, **_):
    output_dir = f"/tmp/wikipageviews/{ts_nodash}"
    pathlib.Path(output_dir).mkdir(parents=True, exist_ok=True)
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    request.urlretrieve(url, f"{output_dir}/wikipageviews.gz")


get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    provide_context=True,
    op_kwargs={
        "year": "{{ execution_date.year }}",
        "month": "{{ execution_date.month }}",
        "day": "{{ execution_date.day }}",
        "hour": "{{ execution_date.hour }}",
    },
    dag=dag,
    depends_on_past=True,
)

extract_gz = BashOperator(
    task_id="extract_gz",
    bash_command="gunzip --force /tmp/wikipageviews/{{ ts_nodash }}/wikipageviews.gz",
    dag=dag,
)


def _fetch_pageviews(pagenames, ts_nodash, execution_date, **_):
    result = dict.fromkeys(pagenames, 0)
    with open(f"/tmp/wikipageviews/{ts_nodash}/wikipageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts

    with open(
        f"/tmp/wikipageviews/{ts_nodash}/postgres_query-{ts_nodash}.sql", "w"
    ) as f:
        for pagename, pageviewcount in result.items():
            f.write(
                "INSERT INTO pageview_counts VALUES ("
                f"'{pagename}', {pageviewcount}, '{execution_date}'"
                ");\n"
            )


fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={"pagenames": {"Google", "Amazon", "Apple", "Microsoft", "Facebook"}},
    provide_context=True,
    dag=dag,
)

write_to_postgres = PostgresOperator(
    task_id="write_to_postgres",
    postgres_conn_id="my_postgres",
    sql="postgres_query-{{ ts_nodash }}.sql",
    dag=dag,
)

get_data >> extract_gz >> fetch_pageviews >> write_to_postgres
