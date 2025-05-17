import os
import shutil
from datetime import datetime, timedelta
import logging

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.bash_operator import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook

ERGAST_FOLDER = "/opt/ergast"
TMP_FOLDER = "/opt/airflow/tmp/ergast"
ERGAST_DW_CONN_ID = "ergast_dw_postgres"

# Telemetry data is available from 2018 in the FastF1 API, but 2025 is not available in Ergast, so skip it.
MIN_YEAR = 2018
MAX_YEAR = 2024

XCOM_FILES_KEY = "processed_files_info"

def setup_directories_callable():
    if os.path.exists(TMP_FOLDER):
        shutil.rmtree(TMP_FOLDER)
    os.makedirs(TMP_FOLDER, exist_ok=True)
    if not os.path.exists(ERGAST_FOLDER) or not os.listdir(ERGAST_FOLDER):
        raise FileNotFoundError(f"ERGAST_FOLDER {ERGAST_FOLDER} not found or empty.")
    logging.info(f"Directories ready. TMP_FOLDER: {TMP_FOLDER}")

def extract_csv_to_parquet_callable(**kwargs):
    processed_files_info = []
    for filename in os.listdir(ERGAST_FOLDER):
        if filename.lower().endswith(".csv"):
            table_name = os.path.splitext(filename)[0].lower()
            csv_path = os.path.join(ERGAST_FOLDER, filename)
            parquet_path = os.path.join(TMP_FOLDER, f"{table_name}.parquet")
            try:
                df = pd.read_csv(csv_path, keep_default_na=True, na_values=['\\N', 'NA', 'N/A', ''])
                df.columns = [col.lower().replace(' ', '_').replace('.', '_') for col in df.columns]
                df.to_parquet(parquet_path, index=False)
                processed_files_info.append({"table_name": table_name, "parquet_path": parquet_path})
            except Exception as e:
                logging.error(f"Error processing {filename} to Parquet: {e}")
                raise

    if not processed_files_info:
        raise AirflowSkipException("No CSV files found to process.")
    kwargs['ti'].xcom_push(key=XCOM_FILES_KEY, value=processed_files_info)
    logging.info(f"Extracted {len(processed_files_info)} files to Parquet.")

def clean_missing_data_callable(**kwargs):
    processed_files_info = kwargs['ti'].xcom_pull(task_ids="extract_task", key=XCOM_FILES_KEY)
    if not processed_files_info:
        raise AirflowSkipException("No files to clean.")

    for file_info in processed_files_info:
        table_name = file_info["table_name"]
        parquet_path = file_info["parquet_path"]
        try:
            df = pd.read_parquet(parquet_path)
            if df.empty:
                logging.info(f"Skipping null cleaning for {table_name} as it is empty.")
                continue

            original_rows = len(df)
            df.dropna(inplace=True)
            cleaned_rows = len(df)

            df.to_parquet(parquet_path, index=False)
            logging.info(f"Cleaned nulls for {table_name} by dropping rows. Original: {original_rows}, Cleaned: {cleaned_rows}. Shape: {df.shape}")
        except Exception as e:
            logging.error(f"Error cleaning data for {table_name}: {e}")
            raise

def load_cleaned_data_to_postgres_callable(**kwargs):
    processed_files_info = kwargs['ti'].xcom_pull(task_ids="extract_task", key=XCOM_FILES_KEY)
    if not processed_files_info:
        raise AirflowSkipException("No data to load to Postgres.")

    pg_hook = PostgresHook(postgres_conn_id=ERGAST_DW_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    for file_info in processed_files_info:
        table_name = file_info["table_name"]
        parquet_path = file_info["parquet_path"]

        try:
            df = pd.read_parquet(parquet_path)

            df.to_sql(
                name=table_name,
                con=engine,
                if_exists='replace',
                index=False,
                chunksize=1000
            )
            logging.info(f"Loaded data into {table_name} (if_exists=replace).")
        except FileNotFoundError:
            logging.warning(f"Parquet file {parquet_path} not found for table {table_name}. Skipping load.")
        except Exception as e:
            logging.error(f"Error loading data into {table_name}: {e}")
            raise

FILTER_DATA_SQL = "\n".join([
	f"DELETE FROM {table} WHERE raceid IN (SELECT raceid FROM races WHERE year < {MIN_YEAR} OR year > {MAX_YEAR});"
	for table in [
		"results", "qualifying", "lap_times", "pit_stops",
		"driver_standings", "constructor_standings", "sprint_results"
	]
] + [
	f"DELETE FROM races WHERE year < {MIN_YEAR} OR year > {MAX_YEAR};",
	f"DELETE FROM seasons WHERE year < {MIN_YEAR} OR year > {MAX_YEAR};"
])


with DAG(
    dag_id='Ergast_ETL',
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    },
    description='Loads Ergast data, drops rows with nulls, recreates DB, loads to DB, then filters by year in DB using SQL.',
    start_date=datetime(2025, 1, 1),
    catchup=True,
	schedule=None, # To run when airflow is set up
) as dag:

    setup_task = PythonOperator(
        task_id="setup_directories_task",
        python_callable=setup_directories_callable,
    )

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_csv_to_parquet_callable,
    )

    # clean_nulls_task = PythonOperator(
    #     task_id="clean_missing_data_task",
    #     python_callable=clean_missing_data_callable,
    # )

    load_to_db_task = PythonOperator(
        task_id="load_cleaned_data_to_postgres_task",
        python_callable=load_cleaned_data_to_postgres_callable,
    )

    filter_in_db_task = SQLExecuteQueryOperator(
        task_id="filter_data_in_postgres_task",
        conn_id=ERGAST_DW_CONN_ID,
        sql=FILTER_DATA_SQL,
    )

    cleanup_task = cleanup_task = BashOperator(
        task_id="cleanup_task",
        bash_command=f'rm -rf {TMP_FOLDER}',
    )


    setup_task >> extract_task >> load_to_db_task >> filter_in_db_task >> cleanup_task
    # setup_task >> extract_task >> clean_nulls_task >> load_to_db_task >> filter_in_db_task