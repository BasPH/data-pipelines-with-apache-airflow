ARG AIRFLOW_BASE_IMAGE="apache/airflow:1.10.12-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}

USER airflow
RUN pip install --user --no-cache-dir apache-airflow-backport-providers-google==2020.10.5
