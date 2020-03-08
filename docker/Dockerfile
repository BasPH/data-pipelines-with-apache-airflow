FROM continuumio/miniconda3:4.7.12

ENV SLUGIFY_USES_TEXT_UNIDECODE=yes \
	PYTHONDONTWRITEBYTECODE=1 \
	AIRFLOW__CORE__LOAD_EXAMPLES=False \
	AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True \
	AIRFLOW__WEBSERVER__DAG_DEFAULT_VIEW=graph \
	AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=15

COPY docker/entrypoint.sh /root/scripts/entrypoint.sh
COPY dags /root/airflow/dags
COPY src /airflowbook/src
COPY setup.py /airflowbook

RUN apt update && \
    apt install -y gcc postgresql-client && \
    pip install --no-cache-dir apache-airflow[crypto,postgres,s3]==1.10.7 && \
    pip install -e /airflowbook

EXPOSE 8080

ENTRYPOINT ["/bin/bash", "/root/scripts/entrypoint.sh"]
