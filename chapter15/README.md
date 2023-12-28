# Chapter 15

Code accompanying Chapter 15 of the book [Data Pipelines with Apache Airflow](https://www.manning.com/books/data-pipelines-with-apache-airflow).

The chapter explains the different options of deploying Airflow in Kubernetes. For executing these deployment commands a docker compose based Kubernetes cluster is available in this chapters `docker-compose.yml` provided with this repository. To start this cluster setup the following command can be used:

```bash
docker-compose up -d
```

!! **This setup requires more resources so it is good to at least give docker 4 CPU and 8GB memory**

## More information

To work with the kubernetes cluster a separate container is available to execute `kubectl` and `helm` commands against the cluster

```bash
docker exec -ti chapter15-k3s-cli-1 /bin/bash
```

### Deployment of default airflow in K8S

Inside the k3s-cli container we can deploy airflow with the following commands:

```bash
helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace
```

to verify the running services/pods we can check with the follwong command:

```bash
kubectl --namespace airflow get pods
```