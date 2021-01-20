# Secrets backend

Demonstrate how to use the secrets backend, connecting with HashiCorp Vault.

## Usage

Running this demo involves a series of steps:

1. `docker-compose up -d`
2. Login in HashiCorp Vault (http://localhost:8200) with token `airflow`.
3. Under `secret/`, create a new secret with:
    - path `connections/secure_api`
    - key `conn_uri`
    - value `http://secure_api:5000?token=supersecret`
4. Go to Airflow (http://localhost:8080), and trigger the DAG "secretsbackend_with_vault"
5. It should succeed, showing "Welcome!" in the logs. Debug logging was enabled so you should also see some
calls to Vault in the logs.

## Details

Exposed ports on host:
- 5432: PostgreSQL (user=airflow, pass=airflow)
- 8080: Airflow webserver
- 8200: HashiCorp Vault
