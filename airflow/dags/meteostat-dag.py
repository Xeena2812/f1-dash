import os
import pendulum
import requests
import pandas as pd
import logging
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

TEMP_DIR = "/opt/airflow/tmp/meteostat"
STATIONS_GZ_FILE_PATH = f"{TEMP_DIR}/stations_full.json.gz"
HOURLY_DATA_URL_TEMPLATE = "https://bulk.meteostat.net/v2/hourly/{station_id}.csv.gz"
METEOSTAT_DW_CONN_ID = "postgres_default"
ERGAST_DW_CONN_ID = "ergast_dw_conn_id"

def _load_stations_to_postgres():
    df = pd.read_json(STATIONS_GZ_FILE_PATH[:-3])
    hook = PostgresHook(METEOSTAT_DW_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    df.to_sql('weather_stations', engine, if_exists='replace', index=False)


def _find_closest_stations(**kwargs):
    target_hook = PostgresHook(METEOSTAT_DW_CONN_ID)

    sql_query = f"""
    SELECT
        c."circuitId",
        ws.id AS closest_station_id,
        ST_Distance(
            ST_MakePoint(c.lng, c.lat)::geography,
            ST_MakePoint(ws.longitude, ws.latitude)::geography
        ) AS distance_meters
    FROM
        circuits c,
        LATERAL (
            SELECT ws2.id, ws2.latitude, ws2.longitude
            FROM weather_stations ws2
            ORDER BY
                ST_Distance(
                    ST_MakePoint(c.lng, c.lat)::geography,
                    ST_MakePoint(ws2.longitude, ws2.latitude)::geography
                )
            LIMIT 1
        ) ws;
    """
    closest_stations_df = target_hook.get_pandas_df(sql_query)

    if not closest_stations_df.empty:
        closest_stations_df.to_sql(
            'circuit_closest_stations',
            target_hook.get_sqlalchemy_engine(),
            if_exists='replace',
            index=False
        )
        station_ids = closest_stations_df['closest_station_id'].unique().tolist()
        kwargs['ti'].xcom_push(key='station_ids', value=station_ids)


def _download_hourly_data(**kwargs):
    downloaded_files = []
    os.makedirs(TEMP_DIR, exist_ok=True)
    station_ids = kwargs['ti'].xcom_pull(task_ids='_find_closest_stations', key='station_ids')

    if not station_ids:
        logging.warning("No station IDs provided for download.")

    for station_id in station_ids:
        url = HOURLY_DATA_URL_TEMPLATE.format(station_id=station_id)
        filename = f"{station_id}.csv.gz"
        filepath = os.path.join(TEMP_DIR, filename)

        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()

            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            downloaded_files.append(filepath)

        except Exception as e:
            logging.warning(f"Could not download data for station {station_id}: {e}")

    kwargs['ti'].xcom_push(key='downloaded_files', value=downloaded_files)


def _load_hourly_data_to_postgres(**kwargs):
    all_hourly_data = []
    downloaded_files = kwargs['ti'].xcom_pull(task_ids='_download_hourly_data', key='downloaded_files')

    if not downloaded_files:
        logging.warning("No files provided for loading hourly data.")
        return

    hourly_columns = [
        'date', 'hour', 'temp', 'dwpt', 'rhum', 'prcp', 'snow', 'wdir',
        'wspd', 'wpgt', 'pres', 'tsun', 'coco'
    ]

    for filepath in downloaded_files:
        try:
            df = pd.read_csv(filepath, compression='gzip', header=0, names=hourly_columns)
            df['date'] = pd.to_datetime(df['date'])
            all_hourly_data.append(df)
        except Exception as e:
            logging.warning(f"Could not process file {filepath}: {e}")

    if not all_hourly_data:
        logging.warning("No hourly data files were successfully processed.")
        return

    combined_df = pd.concat(all_hourly_data, ignore_index=True)
    combined_df = combined_df[['date' > datetime(2018, 1, 1) & 'date' < datetime(2024, 12, 31)]]
    hook = PostgresHook(METEOSTAT_DW_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    combined_df.to_sql('weather_data', engine, if_exists='replace', index=False, chunksize=10000)


with DAG(
    dag_id='Meteostat_ETL',
	default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    },
 	start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    is_paused_upon_creation=False,
) as dag:

    download_stations_gz = BashOperator(
        task_id='download_stations_gz',
        bash_command=f'mkdir -p {TEMP_DIR} && curl -o {STATIONS_GZ_FILE_PATH} https://bulk.meteostat.net/v2/stations/full.json.gz',
    )

    unzip_stations_gz = BashOperator(
        task_id='unzip_stations_gz',
        bash_command=f'gunzip -f {STATIONS_GZ_FILE_PATH}',
    )

    load_stations_task = PythonOperator(
        task_id='load_stations_to_postgres',
        python_callable=_load_stations_to_postgres
    )

    find_closest_stations_task = PythonOperator(
        task_id='find_closest_stations',
        python_callable=_find_closest_stations,
    )

    download_hourly_data_task = PythonOperator(
        task_id='download_hourly_data',
        python_callable=_download_hourly_data,
    )

    load_hourly_data_task = PythonOperator(
        task_id='load_hourly_data_to_postgres',
        python_callable=_load_hourly_data_to_postgres,
    )

    download_stations_gz >> unzip_stations_gz >> load_stations_task >> find_closest_stations_task >> download_hourly_data_task >> load_hourly_data_task