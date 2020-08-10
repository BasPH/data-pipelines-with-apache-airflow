# Chapter 12

Code accompanying Chapter 12 of the book 'Data Pipelines with Apache Airflow'.

## Contents

This code example contains the following:

- /DAGs:
    - dag_failure_callback.py - Trigger a callback function when a DAG run fails.
    - dag_puller_dag.py - Fetches latest code from a Git master branch every 5 minutes (Git must be configured).
    - task_failure_callback.py - Trigger a callback function when a task fails.
    - task_failure_email.py - Send an email when a task fails (SMTP must be configured).
    - task_sla.py - Send an SLA miss notification in case of a missed SLA.
- /docker: An example (base) Dockerfile for use in a CI/CD pipeline.
- /monitoring_docker_compose: A docker-compose setup for monitoring Airflow with StatsD, Prometheus, and Grafana.

## Usage

To get started with the code examples, start Airflow in docker using the following command:

    docker-compose up -d --build

Wait for a few seconds and you should be able to access the examples at http://localhost:8080/.

To stop running the examples, run the following command:

    docker-compose down
