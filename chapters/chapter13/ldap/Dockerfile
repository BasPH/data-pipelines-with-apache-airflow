ARG AIRFLOW_BASE_IMAGE="apache/airflow:2.0.0-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}

USER root

RUN apt-get update && \
    apt-get install -y gcc libsasl2-dev libldap2-dev python-dev libssl-dev && \
    rm -rf /var/lib/apt/lists/* && \
    pip install python-ldap~=3.2.0

USER airflow
