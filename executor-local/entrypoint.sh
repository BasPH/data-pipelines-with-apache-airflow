#!/usr/bin/env bash

# Wait for Postgres to be online
until psql "${AIRFLOW__CORE__SQL_ALCHEMY_CONN}" -c '\q'; do
  >&2 echo "Postgres is unavailable - waiting..."
  sleep 1
done

>&2 echo "Postgres is up - starting Airflow."

airflow initdb
airflow scheduler &
airflow webserver
