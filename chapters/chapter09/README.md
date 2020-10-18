# Chapter 9

Code accompanying Chapter 9 (about testing) of the book [Data Pipelines with Apache Airflow](https://www.manning.com/books/data-pipelines-with-apache-airflow).

## Contents

Chapter 9 discusses the topic of testing. In this folder we have a few DAGs for demonstration purposes,
however there's not really a point in actually running them. Nevertheless, a Docker Compose file was added
which sets up Airflow and allows you to run the DAGs.

The more interesting thing for this chapter are the tests. A custom Docker image was built which includes all
the pytest goodies explained in the chapter. A custom package with operators and hooks is also installed,
named `airflowbook`.

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
