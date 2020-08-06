FROM continuumio/miniconda3:4.7.12

ENV SLUGIFY_USES_TEXT_UNIDECODE=yes \
	PYTHONDONTWRITEBYTECODE=1 \
	AIRFLOW__CORE__LOAD_EXAMPLES=False \
	AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True \
	AIRFLOW__WEBSERVER__DAG_DEFAULT_VIEW=graph \
	AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=15

RUN mkdir -p /root/airflow
COPY entrypoint.sh /root/airflow

RUN apt update && \
    apt install -y postgresql-client curl && \
	conda install -y pytest setproctitle psutil && \
    pip install --no-cache-dir \
		apache-airflow[crypto,postgres]==1.10.5 \
		werkzeug==0.16.0 \
		SQLAlchemy==1.3.15 \
		pytest-mock pytest-helpers-namespace

COPY airflowbook /tmp/airflowbook
RUN pip install /tmp/airflowbook[dev]

EXPOSE 8080

ENTRYPOINT ["/bin/bash", "/root/airflow/entrypoint.sh"]
