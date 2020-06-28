import datetime as dt
from os import path
import tempfile

import pandas as pd

from airflow import DAG, utils as airflow_utils


from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.operators.python_operator import PythonOperator

from custom.ratings import fetch_ratings


QUERY = """
SELECT
    movieId, AVG(rating) as avg_rating, COUNT(*) as num_ratings
FROM OPENROWSET(
    BULK 'https://airflowazure.blob.core.windows.net/airflowazure/*/*.csv',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIELDTERMINATOR =',',
    ROWTERMINATOR = '\n'
)
WITH (
    [userId] bigint,
    [movieId] bigint,
    [rating] float,
    [timestamp] bigint
) AS [r]
WHERE (
    (r.filepath(1) < '2015') OR
    (r.filepath(1) = '2015' AND r.filepath(2) <= '2')
)
GROUP BY movieId
ORDER BY avg_rating DESC
"""

with DAG(
    dag_id="chapter13_azure_usecase",
    description="DAG demonstrating some Azure hooks and operators.",
    start_date=dt.datetime(year=2015, month=1, day=1),
    end_date=dt.datetime(year=2015, month=3, day=1),
    schedule_interval="@monthly",
    default_args={
        "depends_on_past": True
    }
) as dag:

    def _upload_ratings(wasb_conn_id, **context):
        year = context["execution_date"].year
        month = context["execution_date"].month

        # Fetch ratings from our 'API'.
        ratings = fetch_ratings(year=year, month=month)

        # Write ratings to temp file.
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = path.join(tmp_dir, "ratings.csv")
            ratings.to_csv(tmp_path, index=False)

            # Upload file to Azure Blob .
            hook = WasbHook(wasb_conn_id)
            hook.load_file(
                tmp_path,
                container_name="ratings",
                blob_name=f"{year}/{month}.csv",
            )

    upload_ratings = PythonOperator(
        task_id="upload_ratings",
        python_callable=_upload_ratings,
        op_kwargs={
            "wasb_conn_id": "my_wasb_conn",
        },
        provide_context=True,
    )

    def _rank_movies(odbc_conn_id, **context):
        odbc_hook = OdbcHook(
            odbc_conn_id,
            driver="ODBC Driver 17 for SQL Server",
        )

        with odbc_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(QUERY)

                rows = cursor.fetchall()
                colnames = [field[0] for field in cursor.description]

        result = pd.DataFrame.from_records(rows, columns=colnames)

        print(result.head())


    rank_movies = PythonOperator(
        task_id="rank_movies",
        python_callable=_rank_movies,
        op_kwargs={
            "odbc_conn_id": "my_odbc_conn",
        },
        provide_context=True,
    )


    # rank_movies = DatabricksRunNowOperator(
	# 	task_id='notebook_1',
    #     databricks_conn_id="my_databricks_conn",
	# 	job_id=1,
	# 	json= {
	# 		"notebook_params": {
	# 			'inPath': '/bronze/uber'
	# 		}
	# 	}
    # )

    upload_ratings >> rank_movies
