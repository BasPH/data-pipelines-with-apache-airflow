# Chapter 17 - Airflow on Azure

Code accompanying Chapter 17 of the book 'Data pipelines with Apache Airflow'.

## Contents

This code example contains the following files:

```
├── dags
│   ├── 01_azure_usecase.py      # The actual DAG.
│   └── custom                   # Code supporting the DAG.
│       ├── __init__.py
│       └── hooks.py
├── docker
│   └── airflow-azure            # Custom Airflow image with the required depedencies.
├── docker-compose.yml           # Docker-compose file for Airflow.
└──  readme.md                   # This file.
```

## Usage

To get started with the code example, first use the Azure Portal to create the following required resources for the DAG:

* Resource group
* Synapse workspace
* Blob storage containers

How to create these resources (+ what settings to used) is described in the Chapter.

Once the required resources have been created, rename the file .env.template to .env and enter the details of the created resources.

Once this is all set up, you can start Airflow using:

    docker-compose up --build

Once you're done, you can tear down Airflow using:

    docker compose down -v

Don't forget to clean up your Azure resources by deleting the created stack.
