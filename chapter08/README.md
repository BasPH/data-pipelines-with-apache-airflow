# Chapter 8

Code accompanying Chapter 8 of the book 'Data pipelines with Apache Airflow'.

## Contents

This code example contains the following DAGs:

- 01_python.py - Our initial DAG in which we illustrate the use case using the built-in PythonOperator.
- 02_hook.py - Adjusted version of the former DAG, in which we use a custom Airflow hook for connecting to the Movie API.
- 03_operator.py - Another version of the same DAG, using a custom operator class instead of the builtin PythonOperator.
- 04_sensor.py - Final version of the DAG, in which we also demonstrate how to build a custom sensor class.

Besides this, the example also contains the following files:

```
├── api                         <- Docker image for the movie API.
├── dags                        <- Folder containing our DAGs.
│   ├── custom                  <- Custom hooks, etc. used in the DAGs.
│   │   ├── __init__.py
│   │   ├── hooks.py
│   │   ├── operators.py
│   │   ├── ranking.py
│   │   └── sensors.py
│   └── *.py                    <- The DAGs mentioned above.
├── docker-compose.yml
├── src
│   └── airflow-movielens       <- Same code as the 'custom' directory,
│       ├── setup.py               built as a proper Python package.
│       └── src
│           └── airflow_movielens
│               ├── __init__.py
│               ├── hooks.py
│               ├── operators.py
│               └── sensors.py
└── readme.md                   <- This file.
```

## Usage

To get started with the code examples, start Airflow in docker using the following command:

    docker-compose up -d --build

Wait for a few seconds and you should be able to access the examples at http://localhost:8080/.

To stop running the examples, run the following command:

    docker-compose down
