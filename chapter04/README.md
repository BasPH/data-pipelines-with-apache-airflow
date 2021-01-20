# Chapter 4

Code accompanying Chapter 4 of the book [Data Pipelines with Apache Airflow](https://www.manning.com/books/data-pipelines-with-apache-airflow).

## Contents

This folder contains DAGs from Chapter 4. The filenames and DAG ids follow the listing ids in the book. Near
the end of the chapter, we demonstrate usage of the PostgresOperator. The Docker Compose example in this
folder creates a second Postgres database so you don't have to setup things yourself when running the example.
If you like, you can access it:

- Host: `localhost`
- Port: `5433`
- Username: `airflow`
- Password: `airflow`
- Database: `airflow`

This database is initialized with the `pageview_counts` table as shown in the book.

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
