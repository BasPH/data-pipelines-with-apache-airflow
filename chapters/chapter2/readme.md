# Chapter 2

Code accompanying Chapter 2 of the book 'Data pipelines with Apache Airflow'.

## Contents

This code example contains the following DAGs:

- chapter2_download_rocket_launches.py - Small DAG for fetching rocket launches.

## Usage

To get started with the code examples, start Airflow in docker using the following command:

    docker-compose up -d --build

Wait for a few seconds and you should be able to access the examples at http://localhost:8080/.

To stop running the examples, run the following command:

    docker-compose down
