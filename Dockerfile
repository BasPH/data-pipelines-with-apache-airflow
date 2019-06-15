FROM python:3.7

ENV SLUGIFY_USES_TEXT_UNIDECODE=yes \
	PYTHONDONTWRITEBYTECODE=1 \
	AIRFLOW__CORE__LOAD_EXAMPLES=False \
	AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True \
	AIRFLOW__WEBSERVER__DAG_DEFAULT_VIEW=graph

RUN mkdir -p /root/airflow_book
COPY entrypoint.sh /root/airflow_book
COPY dags /root/airflow/dags

RUN apt update && \
    pip install apache-airflow==1.10.3 && \
    yes | airflow initdb

EXPOSE 8080

ENTRYPOINT ["/bin/bash", "/root/airflow_book/entrypoint.sh"]
