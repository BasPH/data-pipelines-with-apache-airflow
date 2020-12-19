# Chapter 13

Code accompanying Chapter 13 (Securing Airflow) of the book [Data Pipelines with Apache Airflow](https://www.manning.com/books/data-pipelines-with-apache-airflow).

## Contents

This folder holds three example Docker Compose examples:

- `ldap`: Example configuration of the webserver, fetching user credentials from OpenLDAP
- `rbac`: Example running the RBAC interface
- `secretsbackend`: Example fetching secrets from HashiCorp Vault

Each folder holds a Docker Compose file which can be started with `docker-compose up -d`.

## Usage

Read the `README.md` file in the respective directory.
