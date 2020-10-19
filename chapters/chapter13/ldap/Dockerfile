FROM apache/airflow:v1-10-test-python3.7

USER root

RUN apt-get update && \
    apt-get install -y gcc libsasl2-dev libldap2-dev python-dev libssl-dev && \
    rm -rf /var/lib/apt/lists/* && \
    pip install python-ldap~=3.2.0

USER airflow
