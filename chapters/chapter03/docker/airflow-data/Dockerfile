ARG AIRFLOW_BASE_IMAGE="apache/airflow:1.10.12-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}

USER root
RUN mkdir -p /data && chown airflow /data

USER airflow
