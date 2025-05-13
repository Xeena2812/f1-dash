# f1-dash

1. `uv sync`
2. `echo -e "AIRFLOW_UID=$(id -u)" > .env` if on Linux
3. docker compose up
4. airflow can be accessed at localhost:8080 with airflow:airflow login details.