# Chapter 11

Code accompanying Chapter 11 of the book 'Data pipelines with Apache Airflow'.

## Contents

This code example contains the following three DAGs:

- 01_task_factory.py - Illustrates how to use a factory method for creating common patterns of tasks.
- 02_dag_factory.py - Demonstrates how to use a factory method to create multiple instances of similar DAGs.
- 03_sla_misses.py - Shows how to use Airflow SLA functionality in your DAGs to catch issues with long running tasks.

## Usage

To get started with the code examples, start Airflow in docker using the following command:

    docker-compose up -d --build

Wait for a few seconds and you should be able to access the examples at http://localhost:8080/.

To stop running the examples, run the following command:

    docker-compose down -v
