#!/usr/bin/env bash

# Trigger DAG with Airflow CLI
airflow dags trigger listing_6_08 --conf '{"supermarket": 1}'

# Trigger DAG with Airflow REST API
curl -X POST "http://localhost:8080/api/v1/dags/listing_6_08/dagRuns" -H  "Content-Type: application/json" -d '{"conf": {"supermarket": 2}}' --user "admin:admin"
