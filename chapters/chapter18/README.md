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
