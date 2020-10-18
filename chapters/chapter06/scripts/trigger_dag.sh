#!/usr/bin/env bash

airflow trigger_dag chapter6_print_dag_run_conf -c '{"supermarket": 1}'
