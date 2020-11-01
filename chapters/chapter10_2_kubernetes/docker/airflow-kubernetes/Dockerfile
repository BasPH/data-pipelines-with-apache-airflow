ARG AIRFLOW_BASE_IMAGE="apache/airflow:1.10.12-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}

RUN pip install --user --no-cache-dir \
    https://dist.apache.org/repos/dist/dev/airflow/providers/0.0.2a1/apache_airflow_providers_cncf_kubernetes-0.0.2a1-py3-none-any.whl
