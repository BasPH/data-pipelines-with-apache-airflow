# Chapter 5

Code accompanying Chapter 5 of the book 'Data pipelines with Apache Airflow'.

## Contents

This code example contains the following DAGs:

- chapter5_01_start.py - Initial DAG with several tasks.
- chapter5_02_branch_in_function.py - Branching within a function.
- chapter5_03_branch_in_dag.py - Branching within the DAG.
- chapter5_04_branch_in_dag_explicit_join.py - Branching within the DAG with a join.
- chapter5_05_branch_in_function.py - Condition within a function.
- chapter5_06_branch_in_dag.py - Condition within the DAG.
- chapter5_07_trigger_rules.py - DAG illustrating several trigger rules.

## Usage

To get started with the code examples, start Airflow in docker using the following command:

    docker-compose up -d --build

Wait for a few seconds and you should be able to access the examples at http://localhost:8080/.

To stop running the examples, run the following command:

    docker-compose down
