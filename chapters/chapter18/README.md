# Chapter 18 - GCP

Code accompanying Chapter 18 (Airflow on GCP) of the book [Data Pipelines with Apache Airflow](https://www.manning.com/books/data-pipelines-with-apache-airflow).

## Contents

This code example contains the following files:

```
├── Makefile            # Makefile for helping run commands.
├── dags
│   └── gcp.py          # The actual DAG.
├── docker-compose.yml  # Docker-compose file for Airflow.
├── README.md           # This file.
└── scripts
    └── fetch_data.py   # Helper script for fetching data.
```

## Usage

This DAG uses several GCP resources, and expects these to be available. An empty template with variables to be
filled is given in `.env.template`. Copy this file to a new file named `.env`, and fill in the details:

```
GCP_PROJECT=[Name of your GCP project]
GCP_KEY=[JSON key for a service account with permissions to use resources]
RATINGS_BUCKET=[Name of a bucket to which ratings data will be uploaded]
RESULT_BUCKET=[Name of a bucket on which results will be stored]
BIGQUERY_DATASET=
```

For this project to work, you require the following resources:

- A GCP project
- A service account + JSON key to be used by Airflow tasks
- A GCS bucket for storing ratings data
- A GCS bucket for storing result data (this can be the same bucket, different object prefix)
- And a BigQuery dataset


To get started with the code example, first make sure to fetch the required dataset:

```bash
make data/ratings
```

This will download the MovieLens 20M dataset (190MB), extract it, and write partitioned data into the folder
`data/`, partitioned as `yyyy/mm.csv`, for example `data/1996/01.csv`. We will use this data throughout the
example on GCP. Next, use the GCP console (or other tool of choice) to create the following resources for the
DAG:

* GCS bucket

How to create these resources (+ what settings to used) is described in the chapter. Once the required
resources have been created, you can start Airflow to run the DAG:

```bash
make airflow-start
```

The `data/` directory is mounted to the Airflow scheduler container.    

You can tear down all resources using:

```bash
make airflow-stop
```
