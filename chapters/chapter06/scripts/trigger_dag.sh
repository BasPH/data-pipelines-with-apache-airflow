#!/usr/bin/env bash

# Trigger DAG with Airflow CLI
airflow dags trigger listing_6_8 --conf '{"supermarket": 1}'

# Trigger DAG with Airflow REST API
curl -X POST "http://localhost:8080/api/v1/dags/listing_6_8/dagRuns" -H  "Content-Type: application/json" -d '{"conf": {"supermarket": 1}}' --user "admin:admin"
