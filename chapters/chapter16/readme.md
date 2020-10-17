# Chapter 13 - AWS

Code accompanying the AWS section of Chapter 13 in the book 'Data pipelines with Apache Airflow'.

## Contents

This code example contains the following files:

├── Makefile                     # Makefile for helping run commands.
├── dags
│   ├── aws_usecase.py           # The actual DAG.
│   └── custom                   # Code supporting the DAG.
│       ├── __init__.py
│       ├── operators.py
│       └── ratings.py
├── docker-compose.yml           # Docker-compose file for Airflow.
├── readme.md                    # This file.
├── resources
│   └── stack.yml                # CloudFormation template for AWS resources required for the DAG.
└── scripts
    └── fetch_data.py            # Helper script for fetching data.

## Usage

To get started with the code example, first make sure to fetch the required dataset:

    make data/ratings

Next, head over to the CloudFormation section in the AWS Console and use the provided CloudFormation template (*resources/stack.yml*) to create the required AWS resources. See the description in the Chapter for more details how to do so, if you're not yet familiar with the process.

Once the CloudFormation stack has been created, you can start Airflow to run the DAG:

    make airflow-start

You can tear down the Airflow resources using

    make airflow-stop
