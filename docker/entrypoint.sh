#!/usr/bin/env bash

set -x

if [[ ! -z "$AIRFLOW__CORE__EXECUTOR" ]] && [[ "$AIRFLOW__CORE__EXECUTOR" != "SequentialExecutor" ]]; then
    # Wait for Postgres to be online
    until psql "${AIRFLOW__CORE__SQL_ALCHEMY_CONN}" -c '\q'; do
        >&2 echo "Postgres is unavailable - waiting..."
        sleep 1
    done
fi

>&2 echo "Postgres is up - starting Airflow."

airflow initdb
airflow scheduler &
airflow webserver
