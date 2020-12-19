# Chapter 10 - Docker

Code accompanying the Docker part of Chapter 10 from the book 'Data pipelines with Apache Airflow'.

## Contents

This code example starts with an example of a Docker image using the wttr API. You can find this example image under `images/wttr-example`.

Besides this, we also include an example DAG that demonstrates a recommender system based on the movielens dataset using Docker.

## Usage

### wttr image

You can run the wttr example using:

    docker build -t manning-airflow/ch10-wttr-example images/wttr-example
    docker run manning-airflow/ch10-wttr-example Amsterdam

This should kick off the process of building the wttr image and running a container using this image.

### Docker DAG

You can run the example DAG using docker-compose:

    docker-compose up -d --build

Wait for a few seconds and you should be able to access the examples at http://localhost:8080/.

To stop running the examples, run the following command:

    docker-compose down -v
