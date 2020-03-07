.DEFAULT_GOAL := help

# From https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: dockerbuild
dockerbuild: ## Build the Docker image
	docker build -t airflowbook/airflow -f docker/Dockerfile .

.PHONY: dockerrun
dockerrun: ## Run Airflow with SequentialExecutor (single Docker container)
	docker run -d -p 8080:8080 -v `pwd`/dags:/root/airflow/dags --name airflowbook airflowbook/airflow:latest

.PHONY: dockerrun-local
dockerrun-local: ## Run Airflow with LocalExecutor (Docker Compose setup with Airflow & Postgres containers)
	docker-compose -f docker/docker-compose-LocalExecutor.yml up -d

.PHONY: chapter7
chapter7:
	docker-compose -f chapters/chapter7/docker-compose.yml up -d

.PHONY: chapter7-stop
chapter7-stop:
	docker-compose -f chapters/chapter7/docker-compose.yml down

.PHONY: chapter10
chapter10:
	docker-compose -f chapters/chapter10/docker-compose.yml up -d

.PHONY: chapter10-stop
chapter10-stop:
	docker-compose -f chapters/chapter10/docker-compose.yml down
