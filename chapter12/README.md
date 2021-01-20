# Chapter 12

Code accompanying Chapter 12 of the book [Data Pipelines with Apache Airflow](https://www.manning.com/books/data-pipelines-with-apache-airflow).

## Contents

This folder contains DAGs from Chapter 12. Topics covered are monitoring, logging, scaling horizontal, etc. An
accompanying Docker Compose setup was built for demonstration purposes. This includes:

- Airflow (webserver, scheduler, and Celery workers)
- PostgreSQL database for Airflow metastore
- Redis for Celery queue
- Flower, a Celery monitoring tool
- Prometheus, for scraping and storing metrics
- Grafana, for visualizing metrics
- And a Redis & StatsD exporter to expose metrics

Given the number of services, this can become a bit resource-heavy on your machine.

Unfortunately, not everything can be scripted/pre-initialized, especially in Grafana. Therefore, you must add
Prometheus as a datasource and create a dashboard yourself.

## Usage

To get started with the code examples, start Airflow with Docker Compose with the following command:

```bash
docker-compose up -d
```

The webserver initializes a few things, so wait for a few seconds, and you should be able to access the
Airflow webserver at http://localhost:8080.

To stop running the examples, run the following command:

```bash
docker-compose down -v
```
