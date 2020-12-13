# Chapter 5

Code accompanying Chapter 5 of the book 'Data pipelines with Apache Airflow'.

## Contents

This code example contains the following DAGs:

- 01_start.py - Initial DAG with several tasks.
- 02_branch_in_function.py - Branching within a function.
- 03_branch_in_dag.py - Branching within the DAG.
- 04_branch_in_dag_explicit_join.py - Branching within the DAG with a join.
- 05_branch_in_function.py - Condition within a function.
- 06_branch_in_dag.py - Condition within the DAG.
- 07_trigger_rules.py - DAG illustrating several trigger rules.

## Usage

To get started with the code examples, start Airflow in docker using the following command:

    docker-compose up -d --build

Wait for a few seconds and you should be able to access the examples at http://localhost:8080/.

To stop running the examples, run the following command:

    docker-compose down
