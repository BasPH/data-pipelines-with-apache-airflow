.PHONY: dockerbuild
dockerbuild:
	docker build -t airflowbook/airflow -f docker/Dockerfile .

.PHONY: dockerrun
dockerrun:
	docker run -d -p 8080:8080 -v `pwd`/dags:/root/airflow/dags --name airflowbook airflowbook/airflow:latest

.PHONY: dockerrun-local
dockerrun-local:
	docker-compose -f docker/docker-compose-LocalExecutor.yml up -d
