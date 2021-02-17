ARG AIRFLOW_BASE_IMAGE="apache/airflow:1.10.12-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}

USER root
COPY custom /opt/airflow/custom
RUN pip install -e /opt/airflow/custom && \
    pip install pytest~=6.1.1 pytest-helpers-namespace~=2019.1.8 pytest-mock~=3.3.1 pytest-docker-tools~=1.0.0

USER airflow
