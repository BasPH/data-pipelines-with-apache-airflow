# Chapter 11

Code accompanying Chapter 11 of the book 'Data pipelines with Apache Airflow'.

## Contents

This code example starts with an example of a Docker image using the wttr API. You can find this example image under `images/wttr-example`.

Besides this, we also include two example DAGs, one for the Docker example and one for Kubernetes:

- dags/docker/movielens_docker.py - Illustrates the recommender example using Docker.
- dags/docker/movielens_kubernetes.py - Illustrates the recommender example using Kubernetes.

The Docker images used for running these DAGs can also be found in the `images` folder.

## Usage

### wttr image

You can run the wttr example using:

    make run-wttr

This should kick off the process of building the wttr image and running a container using this image.

### Docker DAG

You can run the Docker example using docker-compose:

    docker-compose up -f docker-compose-docker.yml -d --build

Wait for a few seconds and you should be able to access the examples at http://localhost:8080/.

To stop running the examples, run the following command:

    docker-compose down

### Kubernetes DAG

For the Kubernetes example, you first need to make sure you have a Kubernetes cluster set up and can run commands on the cluster using kubectl.

Once you have this in place, you can start creating the required namespace and resources:

    kubectl create namespace airflow
    kubectl --namespace airflow apply -f resources/data-volume.yml

as well as the recommender API service:

    kubectl --namespace airflow apply -f resources/api.yml

You can test if the API is running properly using:

    kubectl --namespace airflow port-forward svc/movielens 8000:80

and opening http://localhost:8080 in the browser (this should show a hello world page from the API).

Once this initial setup is complete, you should be able to run the Kubernetes DAG from within Airflow using docker-compose:

    docker-compose up -f docker-compose-docker.yml -d --build

Note that docker-compose is only used to run the Airflow webserver and scheduler/workers, the jobs themselves will be executed in Kubernetes.

If you run into issues, you can lookup the status of the different Kubernetes pods using:

    kubectl --namespace airflow get pods

For failing pods, you can examine their status using:

    kubectl --namespace describe pod [NAME-OF-POD]
