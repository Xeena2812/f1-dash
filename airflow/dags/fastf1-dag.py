import os
import shutil
from datetime import datetime, timedelta
import logging
import fastf1
import time
import glob

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook


FASTF1_FOLDER = "/opt/fastf1-data"
FASTF1_CACHE_FOLDER = "/opt/fastf1-cache"
TMP_FOLDER = "/opt/airflow/tmp/fastf1"
FASTF1_DW_CONN_ID = "fastf1_dw_postgres"

# Telemetry data is available from 2018 in the FastF1 API, but 2025 is not available in Ergast, so skip it.
MIN_YEAR = 2018
MAX_YEAR = 2024

def get_telemetry_and_lap_data_callable(**context):
    SESSIONS_TO_GET = ['Q', 'R']

    try:
        fastf1.Cache.enable_cache(FASTF1_CACHE_FOLDER)
        logging.info(f"FastF1 cache enabled at: {FASTF1_CACHE_FOLDER}")
    except Exception as e:
        logging.error(f"Error enabling FastF1 cache: {e}")

    os.makedirs(FASTF1_FOLDER, exist_ok=True)
    logging.info(f"CSV data will be saved in: {FASTF1_FOLDER}")

    dag_run = context["dag_run"]
    conf = dag_run.conf

    print("Data from conf:", conf.get("data"))
    year = int(conf.get("year"))
    round = int(conf.get("round"))
    name = conf.get("name")

    # Check if (year, round) is already cached in cached_gps table
    pg_hook = PostgresHook(postgres_conn_id=FASTF1_DW_CONN_ID)
    sql = f"SELECT 1 FROM cached_gps WHERE year = {year} AND round = {round} LIMIT 1"
    result = pg_hook.get_first(sql)
    if result:
        logging.info(f"Data for year={year}, round={round} already cached in cached_gps. Skipping.")
        raise AirflowSkipException(f"Data for year={year}, round={round} already cached.")

    event = fastf1.get_event(year, round)
    print(event["EventName"])
    event_name = event['EventName']
    event_round = event['RoundNumber']
    logging.info(f"Processing Event: {year} - Round {event_round} - {event_name}")

    lap_dfs = []
    for session_name in SESSIONS_TO_GET:
        logging.debug(f"Attempting Session: {session_name}")
        session_identifier = f"{year}_{event_round:02d}_{event_name}_{session_name}"

        try:
            session = fastf1.get_session(year, event_name, session_name)

            session.load(laps=True, weather=False, messages=False, telemetry=True)
            logging.info(f"Loaded basic data for {session_identifier}")

            # precise telemetry is in car_data, pos_data, etc.
            # Check https://docs.fastf1.dev/core.html#fastf1.core.Session for more info
            os.makedirs(TMP_FOLDER, exist_ok=True)

            if hasattr(session, 'laps') and not session.laps.empty:
                lap_dfs.append(session.laps)
                logging.debug(f"Saved laps for {session_identifier}")

                sess_tel_dfs = []
                telemetry_saved = False
                for drv_id in session.drivers:
                    try:
                        drv_laps = session.laps.pick_drivers(drv_id)
                        if not drv_laps.empty:
                            drv_abbr = drv_laps['Driver'].iloc[0]
                            drv_tel = drv_laps.get_telemetry()

                            if not drv_tel.empty:
                                drv_tel = drv_tel.merge(drv_laps[['LapNumber', 'Time']], on='Time', how='left')
                                drv_tel['DriverId'] = drv_id
                                drv_tel['Year'] = year
                                drv_tel['Round'] = event_round
                                drv_tel['Session'] = session
                                if 'Date' in drv_tel.columns:
                                    drv_tel[['DateOnly', 'TimeOnly']] = drv_tel['Date'].str.split(' ', n=1, expand=True)

                                sess_tel_dfs.append(drv_tel)
                                logging.debug(f"Saved telemetry for driver {drv_abbr} in {session_identifier} with shape {drv_tel.shape}")
                                telemetry_saved = True
                            else:
                                logging.debug(f"No telemetry data returned for driver {drv_abbr} in {session_identifier}")
                    except Exception as tel_ex:
                        logging.warning(f"Could not get/save telemetry for driver {drv_id} in {session_identifier}: {tel_ex}")
                if telemetry_saved:
                    logging.info(f"Finished processing telemetry for {session_identifier}")
                else:
                    logging.info(f"No telemetry saved for any driver in {session_identifier}")

                # print(sess_tel_dfs)
                if sess_tel_dfs:
                    sess_tel_dfs_concat = pd.concat(sess_tel_dfs, ignore_index=True)
                    sess_tel_dfs_concat.to_csv(os.path.join(TMP_FOLDER, f'telemetry_{session_identifier}.csv'))
                    logging.info(f"Saved {os.path.join(TMP_FOLDER, f'telemetry_{session_identifier}.csv')}")
        except Exception as e:
            logging.error(f"Got Exception:\n{e}")
            raise e

    if lap_dfs:
        sess_lap_dfs_concat = pd.concat(lap_dfs, ignore_index=True)
        sess_lap_dfs_concat.to_csv(os.path.join(TMP_FOLDER, f"laps_{year}_{round}.csv"))
        logging.info(f"Saved {os.path.join(TMP_FOLDER, f"laps_{year}_{round}.csv")}")

def normalize_telemetry_callable(**context):
    dag_run = context["dag_run"]
    conf = dag_run.conf

    year = int(conf.get("year"))
    round = int(conf.get("round"))
    name = conf.get("name")

    telemetry_files = glob.glob(os.path.join(TMP_FOLDER, 'telemetry_*.csv'))
    print(telemetry_files)
    tel_dfs = []

    for tel_file in telemetry_files:
        try:
            tel_file_split = os.path.splitext(os.path.basename(tel_file))[0].split('_')
            if int(tel_file_split[1]) == year and int(tel_file_split[2]) == round:
                df_tel = pd.read_csv(tel_file)
                tel_dfs.append(df_tel)

        except Exception as e:
            logging.warning(f"Could not normalize telemetry file {tel_file}: {e}")

    if tel_dfs:
        df_concat = pd.concat(tel_dfs, ignore_index=True)
        df_concat.drop(df_concat.columns[[0]], axis=1, inplace=True)
        df_concat.to_csv(os.path.join(TMP_FOLDER, f'telemetry_{year}_{round}.csv'))
        logging.info(f"Normalized telemetry saved for {name} with shape {df_concat.shape}")


def load_data_to_postgres_callable(**context):
    dag_run = context["dag_run"]
    conf = dag_run.conf

    year = int(conf.get("year"))
    round = int(conf.get("round"))
    name = conf.get("name")

    temp_csvs = [f"laps_{year}_{round}.csv", f"telemetry_{year}_{round}.csv"]

    pg_hook = PostgresHook(postgres_conn_id=FASTF1_DW_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    for csv_name in temp_csvs:
        table_name = csv_name.split('_')[0]

        try:
            logging.info(f"Loading {table_name}...")
            df = pd.read_csv(os.path.join(TMP_FOLDER, csv_name))

            df.to_sql(
                name=table_name,
                con=engine,
                if_exists='replace',
                index=False,
                chunksize=10000,
            )
            logging.info(f"Loaded data into {table_name} (if_exists=replace).")
        except Exception as e:
            logging.error(f"Error loading data into {table_name}: {e}")


def udpate_cached_table_callable(**context):
    dag_run = context["dag_run"]
    conf = dag_run.conf

    year = int(conf.get("year"))
    round = int(conf.get("round"))
    name = conf.get("name")

    pg_hook = PostgresHook(postgres_conn_id=FASTF1_DW_CONN_ID)
    sql = "INSERT INTO cached_gps (year, round, name) VALUES (%s, %s, %s)"
    pg_hook.run(sql, parameters=(year, round, name))
    logging.info(f"Inserted (year={year}, round={round}) into cached_gps table.")

with DAG(
    'FastF1_ETL',
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    },
    description='Gets data for a selected weekend from the FastF1 API',
    start_date=datetime(2025, 1, 1),
    catchup=True,
	schedule=None, # To run when airflow is set up
    is_paused_upon_creation=False,
) as dag:
    get_data_for_weekend_task = PythonOperator(
        task_id="get_data_for_weekend_task",
        python_callable=get_telemetry_and_lap_data_callable,
    )
    normalize_telemetry_task = PythonOperator(
        task_id="normalize_telemetry_task",
        python_callable=normalize_telemetry_callable,
    )

    load_to_db_task = PythonOperator(
        task_id="load_cleaned_data_to_postgres_task",
        python_callable=load_data_to_postgres_callable,
    )

    udpate_cached_table = PythonOperator(
        task_id="udpate_cached_table_task",
        python_callable=udpate_cached_table_callable,
    )


    cleanup_task = cleanup_task = BashOperator(
        task_id="cleanup_task",
        bash_command=f'rm -rf {TMP_FOLDER}',
    )

    get_data_for_weekend_task >> normalize_telemetry_task >> load_to_db_task >> cleanup_task
    load_to_db_task >> udpate_cached_table