# Webserver theme demo

This docker-compose file demonstrates settings a webserver theme in `webserver_config.py`.

## Usage

```
docker-compose up -d
```

Wait 5 seconds or so for the webserver to come up.

Login in Airflow username/password `airflow`/`airflow`.

## Details

Exposed ports on host:
- 5432: PostgreSQL (user=`airflow`, pass=`airflow`)
- 8080: Airflow webserver
