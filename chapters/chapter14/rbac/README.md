# RBAC demo

This docker-compose file demonstrates the Airflow RBAC interface.

## Usage

```
docker-compose up -d
```

Wait 5 seconds or so for the webserver to come up (Both the webserver & init containers must run a command,
which must be done in the correct order. Normally this is done by a human, but in the scripts we used a
`sleep(5 seconds)` to ensure the correct ordering, which delays the webserver startup by 5 seconds). 

Login in Airflow username/password `airflow`/`airflow`.

## Details

Exposed ports on host:
- 5432: PostgreSQL (user=`airflow`, pass=`airflow`)
- 8080: Airflow webserver
