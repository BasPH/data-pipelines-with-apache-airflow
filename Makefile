.PHONY: build
build:
	docker build -t basph/airflow-book .

.PHONY: run
run:
	docker run -d -p 8080:8080 basph/airflow-book
