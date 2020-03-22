# Chapter 1

Code accompanying Chapter 1 of the book 'Data pipelines with Apache Airflow'.

## Contents

This code example contains the following DAGs:

- chapter_01_figure1.py - Example DAG shown in Figure 1.
- chapter_02_umbrella_prediction.py - DAG showing the umbrella prediction model (as dummy tasks).

## Usage

To get started with the code examples, start Airflow in docker using the following command:

    docker-compose up -d --build

Wait for a few seconds and you should be able to access the examples at http://localhost:8080/.

To stop running the examples, run the following command:

    docker-compose down
