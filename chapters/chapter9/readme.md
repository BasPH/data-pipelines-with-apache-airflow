# Chapter 9

Code accompanying Chapter 9 of the book 'Data pipelines with Apache Airflow'.

## Contents

This code example contains the following DAGs:

- chapter9 - Small DAG illustrating the postgres-to-s3 operator.

## Usage

To get started with the code examples, start Airflow in docker using the following command:

    docker-compose up -d --build

Wait for a few seconds and you should be able to access the examples at http://localhost:8080/.

To stop running the examples, run the following command:

    docker-compose down
