import csv
import io
import os

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults


class PostgresToS3Operator(BaseOperator):
    template_fields = ("_query", "_s3_key")

    @apply_defaults
    def __init__(
        self, postgres_conn_id, query, s3_conn_id, s3_bucket, s3_key, **kwargs
    ):
        super().__init__(**kwargs)
        self._postgres_conn_id = postgres_conn_id
        self._query = query
        self._s3_conn_id = s3_conn_id
        self._s3_bucket = s3_bucket
        self._s3_key = s3_key

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self._postgres_conn_id)
        s3_hook = S3Hook(aws_conn_id=self._s3_conn_id)

        with postgres_hook.get_cursor() as cursor:
            cursor.execute(self._query)
            results = cursor.fetchall()
            headers = [_[0] for _ in cursor.description]

        data_buffer = io.StringIO()
        csv_writer = csv.writer(
            data_buffer, quoting=csv.QUOTE_ALL, lineterminator=os.linesep
        )
        csv_writer.writerow(headers)
        csv_writer.writerows(results)
        data_buffer_binary = io.BytesIO(data_buffer.getvalue().encode())

        s3_hook.load_file_obj(
            file_obj=data_buffer_binary,
            bucket_name=self._s3_bucket,
            key=self._s3_key,
            replace=True,
        )
