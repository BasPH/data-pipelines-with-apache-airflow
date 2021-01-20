# Chapter 3

Code accompanying Chapter 3 of the book 'Data pipelines with Apache Airflow'.

## Contents

This code example contains the following DAGs:

- 01_unscheduled.py - Initial DAG without schedule.
- 02_daily_schedule.py - Same DAG following a daily schedule.
- 03_different_start_date.py - DAG with adjusted start date.
- 04_with_end_date.py - Modified DAG with an end date.
- 05_time_delta_schedule.py - DAG that uses a timedelta for the schedule interval.
- 06_query_with_dates.py - DAG including hard-coded dates in the query.
- 07_templated_query.py - Replaces hard-coded dates with templated execution dates.
- 08_templated_query_ds.py - Uses shorthands for the templated execution dates.
- 09_templated_path.py - Uses templating for the file paths as well.
- 10_full_example.py - Filly completed example, including 'sending' of statistics.

## Usage

To get started with the code examples, start Airflow in docker using the following command:

    docker-compose up --build

Wait for a few seconds and you should be able to access the examples at http://localhost:8080/.

To stop running the examples, run the following command:

    docker-compose down -v
