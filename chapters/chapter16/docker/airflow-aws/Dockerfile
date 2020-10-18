ARG AIRFLOW_BASE_IMAGE="apache/airflow:1.10.12-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}

# GCC is needed to build a dependency of the backport package.
USER root
RUN apt-get update && \
    apt-get install -y build-essential && \
    rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --user --no-cache-dir \
    apache-airflow-backport-providers-amazon==2020.10.5
