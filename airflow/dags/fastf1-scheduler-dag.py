import logging
from datetime import datetime, timedelta, timezone

import fastf1
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

MIN_YEAR = 2018
MAX_YEAR = 2024
FASTF1_DW_CONN_ID = "fastf1_dw_postgres"

def find_next_event_callable(**context):
    events = []
    for year in range(MIN_YEAR, MAX_YEAR + 1):
        fastf1_events = fastf1.events.get_event_schedule(year)
        for _, row in fastf1_events.iterrows():
            events.append((year, int(row['RoundNumber']), row['EventName']))
    events.sort()

    # Get all cached (year, round) from cached_gps table
    pg_hook = PostgresHook(postgres_conn_id=FASTF1_DW_CONN_ID)
    cached = pg_hook.get_pandas_df("SELECT year, round FROM cached_gps")
    cached_set = set()
    for _, row in cached.iterrows():
        cached_set.add((row['year'], row['round']))

    # Find the first (year, round) not in cached_gps
    for year, rnd, name in events:
        if (year, rnd) not in cached_set:
            logging.info(f"Next event to process: year={year}, round={rnd}, name={name}")
            context['ti'].xcom_push(key='next_event', value={'year': year, 'round': rnd, 'name': name})
            return
    logging.info("No new events to process.")
    context['ti'].xcom_push(key='next_event', value=None)

def ensure_cached_gps_table_callable(**context):
    pg_hook = PostgresHook(postgres_conn_id=FASTF1_DW_CONN_ID)
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS cached_gps (
        year INTEGER NOT NULL,
        round INTEGER NOT NULL,
        name TEXT,
        PRIMARY KEY (year, round)
    );
    """
    pg_hook.run(create_table_sql)
    logging.info("Ensured cached_gps table exists.")

def trigger_fastf1_etl_callable(**context):
    AIRFLOW_API_URL = "http://airflow-apiserver:8080/api/v2/dags/FastF1_ETL/dagRuns"
    AIRFLOW_USERNAME = Variable.get("AIRFLOW_API_USERNAME", default_var="airflow")
    AIRFLOW_PASSWORD = Variable.get("AIRFLOW_API_PASSWORD", default_var="airflow")

    conf = context['ti'].xcom_pull(task_ids='find_next_event', key='next_event')
    if not conf:
        context['ti'].log.info("No event to trigger.")
        return

    auth_response = requests.post(
        "http://airflow-apiserver:8080/auth/token",
        json={"username": AIRFLOW_USERNAME, "password": AIRFLOW_PASSWORD},
        headers={"Content-Type": "application/json"}
    )

    if auth_response.status_code != 200 and auth_response.status_code != 201:
        raise Exception(f"Failed to get auth token: {auth_response.status_code} {auth_response.text}")
    token = auth_response.json().get("access_token")
    if not token:
        raise Exception("No access_token found in auth response.")

    payload = {
        "conf": conf,
        "logical_date" : datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    response = requests.post(
        AIRFLOW_API_URL,
        json=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }
    )
    if response.status_code not in (200, 201):
        raise Exception(f"Failed to trigger DAG: {response.status_code} {response.text}")


with DAG(
    dag_id='FastF1_Scheduler',
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    },
    description='Schedules FastF1 ETL jobs for uncached events',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=timedelta(minutes=5),
    is_paused_upon_creation=False,
) as dag:

    ensure_cached_gps_table = PythonOperator(
        task_id="ensure_cached_gps_table",
        python_callable=ensure_cached_gps_table_callable,
    )

    find_next_event = PythonOperator(
        task_id="find_next_event",
        python_callable=find_next_event_callable,
    )

    trigger_fastf1_etl = PythonOperator(
        task_id="trigger_fastf1_etl",
        python_callable=trigger_fastf1_etl_callable,
    )

    ensure_cached_gps_table >> find_next_event >> trigger_fastf1_etl