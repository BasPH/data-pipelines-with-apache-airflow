# RBAC demo

This docker-compose file demonstrates the Airflow RBAC interface.

## Usage

```
docker-compose up -d
```

Login in Airflow username/password `bobsmith`/`topsecret`.

## Details

Exposed ports on host:
- 5432: PostgreSQL (user=`airflow`, pass=`airflow`)
- 8080: Airflow webserver
