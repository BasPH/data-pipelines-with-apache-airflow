#!/usr/bin/env bash

# This check verifies if the DAGs directory contains a .noresetdb file.
# The Docker image comes with a pre-initialized DB so you don't have to wait on boot for DAG parsing. However,
# if you mount your own DAGs volume, the examples would still be visible in the UI and therefore a resetdb is
# performed to wipe the DB on boot and to parse all DAGs.
if [[ ! -f /root/airflow/dags/.noresetdb ]]; then
    airflow resetdb
fi

airflow scheduler &
airflow webserver
