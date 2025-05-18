import os
import requests
import pandas as pd
import logging
import json
import meteostat
from datetime import datetime, timedelta, date
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

TEMP_DIR = "/opt/airflow/tmp/meteostat"
STATIONS_GZ_FILE_PATH = f"{TEMP_DIR}/stations_full.json.gz"
HOURLY_DATA_URL_TEMPLATE = "https://bulk.meteostat.net/v2/hourly/{station_id}.csv.gz"
METEOSTAT_DW_CONN_ID = "meteostat_dw_postgres"
ERGAST_DW_CONN_ID = "ergast_dw_postgres"

def load_stations_to_postgres():
    with open(STATIONS_GZ_FILE_PATH[:-3], 'r') as f:
        stations_data = json.load(f)

    df = pd.json_normalize(stations_data)
    hook = PostgresHook(postgres_conn_id=METEOSTAT_DW_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    df.to_sql(name='weather_stations', con=engine, if_exists='replace', index=False)


def find_closest_stations(**kwargs):
    ergast_hook = PostgresHook(postgres_conn_id=ERGAST_DW_CONN_ID)
    sql_query = f"""
    SELECT c.circuitid, c.name, c.lat, c.lng FROM circuits c
    """
    circuits = ergast_hook.get_pandas_df(sql_query)
    stations = meteostat.Stations()
    station_ids = []
    print(circuits.shape)
    for _, circuit in circuits.iterrows():
        nearby_stations = stations.nearby(circuit['lat'], circuit['lng'])
        closest_station = nearby_stations.fetch(1)
        station_ids.append(closest_station.index[0])

    print(station_ids)
    kwargs['ti'].xcom_push(key='station_ids', value=station_ids)
    return station_ids


def download_hourly_data(**kwargs):
    downloaded_files = []
    os.makedirs(TEMP_DIR, exist_ok=True)
    station_ids = kwargs['ti'].xcom_pull(task_ids='find_closest_stations', key='station_ids')
    print("Station_ids:", station_ids)
    if not station_ids:
        logging.warning("No station IDs provided for download.")

    for station_id in station_ids:
        url = HOURLY_DATA_URL_TEMPLATE.format(station_id=station_id)
        filename = f"{station_id}.csv.gz"
        filepath = os.path.join(TEMP_DIR, filename)
        if os.path.exists(filepath):
            downloaded_files.append(filepath)
            continue

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


def load_hourly_data_to_postgres(**kwargs):
    all_hourly_data = []
    downloaded_files = kwargs['ti'].xcom_pull(task_ids='download_hourly_data', key='downloaded_files')

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
            df['date'] = pd.to_datetime(df['date']).dt.date
            df['station_id'] = filepath.split('.')[0].split('/')[-1]
            all_hourly_data.append(df)
        except Exception as e:
            logging.warning(f"Could not process file {filepath}: {e}")

    if not all_hourly_data:
        logging.warning("No hourly data files were successfully processed.")
        return

    combined_df = pd.concat(all_hourly_data, ignore_index=True)
    combined_df = combined_df[(combined_df['date'] > date(2018, 1, 1)) & (combined_df['date'] < date(2024, 12, 31))]
    combined_df['date'] = pd.to_datetime(combined_df['date']).dt.strftime('%Y-%m-%d')
    hook = PostgresHook(postgres_conn_id=METEOSTAT_DW_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    combined_df.to_sql('weather_data', engine, if_exists='replace', index=False, chunksize=10000)


with DAG(
    dag_id='Meteostat_ETL',
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    },
     start_date=datetime(2025, 1, 1),
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
        python_callable=load_stations_to_postgres
    )

    find_closest_stations_task = PythonOperator(
        task_id='find_closest_stations',
        python_callable=find_closest_stations,
    )

    download_hourly_data_task = PythonOperator(
        task_id='download_hourly_data',
        python_callable=download_hourly_data,
    )

    load_hourly_data_task = PythonOperator(
        task_id='load_hourly_data_to_postgres',
        python_callable=load_hourly_data_to_postgres,
    )

    cleanup_task = cleanup_task = BashOperator(
        task_id="cleanup_task",
        bash_command=f'rm -rf {TEMP_DIR}',
    )



    download_stations_gz >> unzip_stations_gz >> load_stations_task >> find_closest_stations_task >> download_hourly_data_task >> load_hourly_data_task >> cleanup_task