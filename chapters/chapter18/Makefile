.DEFAULT_GOAL := help

# From https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: airflow-start
airflow-start: ## Spin up Docker containers for running GCP DAG using Docker Compose
	docker-compose up -d

.PHONY: airflow-stop
airflow-stop: ## Tear down Docker Compose setup
	docker-compose down

data/ratings: ## Download the ratings data
	python scripts/fetch_data.py
