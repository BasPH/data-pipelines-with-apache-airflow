.DEFAULT_GOAL := help

# From https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: black
black:  ## Check formatting with black.
	black --check .

.PHONY: chapter1
chapter1:  ## Run Airflow DAGs for Chapter 1.
	docker-compose -f chapters/chapter1/docker-compose.yml up

.PHONY: chapter2
chapter2:  ## Run Airflow DAGs for Chapter 2.
	docker-compose -f chapters/chapter2/docker-compose.yml up

.PHONY: chapter3
chapter3:  ## Run Airflow DAGs for Chapter 3.
	docker-compose -f chapters/chapter3/docker-compose.yml up

.PHONY: chapter4
chapter4:  ## Run Airflow DAGs for Chapter 4.
	docker-compose -f chapters/chapter4/docker-compose.yml up

.PHONY: chapter5
chapter5:  ## Run Airflow DAGs for Chapter 5.
	docker-compose -f chapters/chapter5/docker-compose.yml up

.PHONY: chapter6
chapter6:  ## Run Airflow DAGs for Chapter 6.
	docker-compose -f chapters/chapter6/docker-compose.yml up

.PHONY: chapter7
chapter7:  ## Run Airflow DAGs for Chapter 7.
	docker-compose -f chapters/chapter7/docker-compose.yml up

.PHONY: chapter9
chapter9:  ## Run Airflow DAGs for Chapter 8.
	docker-compose -f chapters/chapter9/docker-compose.yml up

.PHONY: chapter10
chapter10:  ## Run Airflow DAGs for Chapter 10.
	docker-compose -f chapters/chapter10/docker-compose.yml up

.PHONY: docker
docker:  ## Build the Airflow Docker image
	docker build -t manning-airflow:latest docker

.PHONY: flake8
flake8:
	flake8 .

