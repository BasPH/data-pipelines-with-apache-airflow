import logging
from typing import Dict, Callable

import pandas as pd
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from minio import Minio
from sqlalchemy import create_engine


class MinioPandasToPostgres(BaseOperator):
    template_fields = ("_minio_key",)
    ui_color = "#705B74"
    ui_fgcolor = "#8FA48B"

    @apply_defaults
    def __init__(
        self,
        minio_conn_id,
        minio_bucket,
        minio_key,
        pandas_read_callable,
        postgres_conn_id,
        postgres_table,
        read_callable_kwargs: Dict = None,
        pre_read_transform: Callable = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._minio_conn_id = minio_conn_id
        self._minio_bucket = minio_bucket
        self._minio_key = minio_key
        self._pandas_read_callable = pandas_read_callable
        self._postgres_conn_id = postgres_conn_id
        self._postgres_table = postgres_table
        self._read_callable_kwargs = read_callable_kwargs or {}
        self._pre_read_transform = pre_read_transform

    def execute(self, context):
        conn = BaseHook.get_connection(conn_id=self._minio_conn_id)
        minio_client = Minio(
            conn.extra_dejson["host"].split("://")[1],
            access_key=conn.extra_dejson["aws_access_key_id"],
            secret_key=conn.extra_dejson["aws_secret_access_key"],
            secure=False,
        )

        logging.info("Reading object: %s/%s.", self._minio_bucket, self._minio_key)
        obj = minio_client.get_object(
            bucket_name=self._minio_bucket, object_name=self._minio_key
        )
        if self._pre_read_transform:
            obj = self._pre_read_transform(obj)

        df = self._pandas_read_callable(obj, **self._read_callable_kwargs)
        df["airflow_execution_date"] = pd.Timestamp(
            context["execution_date"].timestamp(), unit="s"
        )
        logging.info("Read DataFrame with shape: %s.", df.shape)

        engine = create_engine(
            BaseHook.get_connection(self._postgres_conn_id).get_uri()
        )
        with engine.begin() as conn:
            conn.execute(
                f"DELETE FROM {self._postgres_table} "
                f"WHERE airflow_execution_date='{context['execution_date']}';"
            )
            df.to_sql(self._postgres_table, con=conn, index=False, if_exists="append")

        logging.info("Wrote DataFrame to %s.", self._postgres_table)
