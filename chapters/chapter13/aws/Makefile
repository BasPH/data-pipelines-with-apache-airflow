
data/ratings:
	python scripts/fetch_data.py

.PHONY: airflow-start
airflow-start:
	docker-compose up -d --build

.PHONY: airflow-stop
airflow-stop:
	docker-compose down
