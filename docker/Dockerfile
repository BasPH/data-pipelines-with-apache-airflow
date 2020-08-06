FROM continuumio/miniconda3:4.7.12

ENV SLUGIFY_USES_TEXT_UNIDECODE=yes \
	PYTHONDONTWRITEBYTECODE=1 \
	AIRFLOW__CORE__LOAD_EXAMPLES=False \
	AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True \
	AIRFLOW__WEBSERVER__DAG_DEFAULT_VIEW=graph \
	AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=15

RUN mkdir -p /root/airflow
COPY entrypoint.sh /root/airflow

# Install odbc driver for Synapse.
RUN apt update && \
    apt-get install -y gnupg curl && \
  	curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 mssql-tools
   
# Install other dependencies.
RUN apt install -y postgresql-client 

# Install Airflow + Python dependencies.
RUN conda install -y pytest setproctitle psutil pyodbc && \
    pip install --no-cache-dir \
		apache-airflow[s3,crypto,postgres,kubernetes]==1.10.5 \
		SQLAlchemy==1.3.15 \
		werkzeug==0.16.0 \
		pytest-mock pytest-helpers-namespace \
		apache-airflow-backport-providers-odbc \
		apache-airflow-backport-providers-microsoft-azure \
		apache-airflow-backport-providers-amazon

# Install kubectl. 
RUN curl -LO https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl
RUN chmod +x ./kubectl
RUN mv ./kubectl /usr/local/bin

COPY airflowbook /tmp/airflowbook
RUN pip install /tmp/airflowbook[dev]

EXPOSE 8080

ENTRYPOINT ["/bin/bash", "/root/airflow/entrypoint.sh"]
