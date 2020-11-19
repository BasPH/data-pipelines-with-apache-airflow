ARG AIRFLOW_BASE_IMAGE="apache/airflow:2.0.0b3-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}

RUN pip install --user --no-cache-dir \
    apache-airflow-providers-cncf-kubernetes==1.0.0b2
