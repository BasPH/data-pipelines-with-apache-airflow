# Chapter 10 - Kubernetes

Code accompanying the Kubernetes part of Chapter 10 from the book 'Data pipelines with Apache Airflow'.

## Contents

This directory contains the Kubernetes equivalent of the recommender system demonstrated in the chapter10_1_docker example.

### Usage

First, you need to make sure you have a Kubernetes cluster set up and can run commands on the cluster using kubectl.

Once you have this in place, you can start creating the required namespace and resources:

    kubectl create namespace airflow
    kubectl --namespace airflow apply -f resources/data-volume.yml

as well as the recommender API service:

    docker build -t manning-airflow/movielens-api ../chapter08/docker/movielens-api
    kubectl --namespace airflow apply -f resources/api.yml

You can test if the API is running properly using:

    kubectl --namespace airflow port-forward svc/movielens 8000:80

and opening http://localhost:8080 in the browser (this should show a hello world page from the API).

Once this initial setup is complete, you should be able to run the Kubernetes DAG from within Airflow using docker-compose:

    docker-compose up -f docker-compose.yml -d --build

Note that docker-compose is only used to run the Airflow webserver and scheduler/workers, the jobs themselves will be executed in Kubernetes.

If you run into issues, you can lookup the status of the different Kubernetes pods using:

    kubectl --namespace airflow get pods

For failing pods, you can examine their status using:

    kubectl --namespace describe pod [NAME-OF-POD]

You can tear down any used resources with:

    docker-compose down -v
    kubectl delete namespace airflow
