
# Create namespace.
kubectl create namespace airflow

# Create resources.
kubectl --namespace airflow apply -f resources/data-volume.yml

docker build -t manning-airflow/ch10-movielens-api:latest ../images/movielens-api
kubectl --namespace airflow apply -f resources/api.yml

# Run Airflow in docker-compose.
docker-compose up

# Terminate + remove resources when done.
docker-compose down -v
kubectl delete namespace airflow
