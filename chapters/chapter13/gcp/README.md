# Chapter 13 - GCP

Code accompanying the GCP section of Chapter 13 in the book 'Data pipelines with Apache Airflow'.

## Contents

This code example contains the following files:

├── Makefile            # Makefile for helping run commands.
├── dags
│   └── gcp.py          # The actual DAG.
├── docker-compose.yml  # Docker-compose file for Airflow.
├── README.md           # This file.
└── scripts
    └── fetch_data.py   # Helper script for fetching data.

## Usage

To get started with the code example, first make sure to fetch the required dataset:

    make data/ratings

Next, use the GCP console (or other tool of choice) to create the following resources for the DAG:

* GCS bucket

How to create these resources (+ what settings to used) is described in the Chapter.

Once the required resources have been created, you can start Airflow to run the DAG:

    make airflow-start

You can tear down the Airflow resources using

    make airflow-stop
