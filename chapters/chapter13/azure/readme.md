# Chapter 13 - Azure

Code accompanying the Azure section of Chapter 13 in the book 'Data pipelines with Apache Airflow'.

## Contents

This code example contains the following files:

├── Makefile                     # Makefile for helping run commands.
├── dags
│   ├── azure_usecase.py         # The actual DAG.
│   └── custom                   # Code supporting the DAG.
│       ├── __init__.py
│       └── ratings.py
├── docker-compose.yml           # Docker-compose file for Airflow.
├── readme.md                    # This file.
└── scripts
    └── fetch_data.py            # Helper script for fetching data.

## Usage

To get started with the code example, first make sure to fetch the required dataset:

    make data/ratings

Next, use the Azure Portal to create the following required resources for the DAG:

* Resource group
* Synapse workspace
* Blob storage containers

How to create these resources (+ what settings to used) is described in the Chapter.

Once the required resources have been created, you can start Airflow to run the DAG:

    make airflow-start

You can tear down the Airflow resources using

    make airflow-stop
