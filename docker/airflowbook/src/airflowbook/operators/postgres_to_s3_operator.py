import csv
import io

from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
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

        results = postgres_hook.get_records(self._query)

        data_buffer = io.StringIO()
        csv_writer = csv.writer(data_buffer)
        csv_writer.writerows(results)
        data_buffer_binary = io.BytesIO(data_buffer.getvalue().encode())

        s3_hook.get_conn().upload_fileobj(
            Fileobj=data_buffer_binary, Bucket=self._s3_bucket, Key=self._s3_key
        )
