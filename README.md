# Data Pipelines with Apache Airflow

Code for the book [Data Pipelines with Apache Airflow](https://www.manning.com/books/data-pipelines-with-apache-airflow).

It comes with a supporting Docker image. Either build yourself with `make dockerbuild` and run with
`make dockerrun` (runs SequentialExecutor by default), or run from Docker Hub with `docker run airflowbook`.

A Docker Compose file using the LocalExecutor with a separate Postgres container is also available by running
`make dockerrun-local` or `docker-compose -f docker/docker-compose-LocalExecutor.yml up`.
