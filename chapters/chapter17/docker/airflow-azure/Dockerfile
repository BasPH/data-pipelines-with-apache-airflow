ARG AIRFLOW_BASE_IMAGE="apache/airflow:1.10.12-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}

# Install:
#   - odbc driver for Synapse
#   - gcc for building a Python dependency
#   - unixodbc for pyodbc
USER root
RUN apt-get update && \
    apt-get install -y gnupg curl build-essential unixodbc unixodbc-dev && \
  	curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 mssql-tools && \
    rm -rf /var/lib/apt/lists/*

# Install Azure backport package.
USER airflow
RUN pip install --user --no-cache-dir \
    https://dist.apache.org/repos/dist/dev/airflow/providers/0.0.2a1/apache_airflow_providers_odbc-0.0.2a1-py3-none-any.whl \
    https://dist.apache.org/repos/dist/dev/airflow/providers/0.0.2a1/apache_airflow_providers_microsoft_azure-0.0.2a1-py3-none-any.whl
