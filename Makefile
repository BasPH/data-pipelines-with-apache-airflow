.PHONY: build
build:
	docker build -t basph/airflow-book -f executor-sequential/Dockerfile .

.PHONY: run
run:
	docker run -d -p 8080:8080 -v `pwd`/dags:/root/airflow/dags basph/airflow-book:latest

.PHONY: build-local
build-local:
	docker build -t basph/airflow-book:local -f executor-local/Dockerfile .

.PHONY: run-local
run-local:
	docker-compose -f executor-local/docker-compose.yml up -d
