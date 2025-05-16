import os
import shutil
from datetime import datetime, timedelta
import logging
import fastf1
import time

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook


FASTF1_FOLDER = "/opt/fastf1-data"
FASTF1_CACHE_FOLDER = "/opt/fastf1-cache"
TMP_FOLDER = "/opt/airflow/tmp/fastf1"
ERGAST_DW_CONN_ID = "fastf1_dw_postgres"

# Telemetry data is available from 2018 in the FastF1 API, but 2025 is not available in Ergast, so skip it.
MIN_YEAR = 2018
MAX_YEAR = 2024

XCOM_FILES_KEY = "processed_files_info"
"""
def setup_directories_callable():
    if os.path.exists(TMP_FOLDER):
        shutil.rmtree(TMP_FOLDER)
    os.makedirs(TMP_FOLDER, exist_ok=True)
    if not os.path.exists(FASTF1_FOLDER) or not os.listdir(FASTF1_FOLDER):
        raise FileNotFoundError(f"ERGAST_FOLDER {FASTF1_FOLDER} not found or empty.")
    logging.info(f"Directories ready. TMP_FOLDER: {TMP_FOLDER}")

def extract_csv_to_parquet_callable(**kwargs):
    processed_files_info = []
    for filename in os.listdir(FASTF1_FOLDER):
        if filename.lower().endswith(".csv"):
            table_name = os.path.splitext(filename)[0].lower()
            csv_path = os.path.join(FASTF1_FOLDER, filename)
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
"""
def get_data_for_weekend_callable(**context):
	SESSIONS_TO_GET = ['FP1', 'FP2', 'FP3', 'Q', 'S', 'SQ', 'R']

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


"""

def get_data_from_api(**kwargs):
	SESSIONS_TO_GET = ['FP1', 'FP2', 'FP3', 'Q', 'S', 'SQ', 'R'] # Common sessions (Sprint='S', Sprint Quali='SQ')
	DELAY_SECONDS = 1
 # Delay between processing sessions to be polite to API
	try:
		fastf1.Cache.enable_cache(FASTF1_CACHE_FOLDER)
		logging.info(f"FastF1 cache enabled at: {FASTF1_CACHE_FOLDER}")
	except Exception as e:
		logging.error(f"Error enabling FastF1 cache: {e}")

	os.makedirs(FASTF1_FOLDER, exist_ok=True)
	logging.info(f"CSV data will be saved in: {FASTF1_FOLDER}")


	for year in range(MIN_YEAR, MAX_YEAR + 1):
		logging.debug(f"--- Processing Year: {year} ---", )
		try:
			schedule = fastf1.get_event_schedule(year)
			# Convert EventDate to datetime objects to filter past events if needed
			# schedule['EventDate'] = pd.to_datetime(schedule['EventDate']).dt.date
			# schedule = schedule[schedule['EventDate'] < datetime.now().date()] # Optional: Only process past events

		except Exception as e:
			logging.error(f"Could not get event schedule for {year}: {e}")
			continue

		for index, event in schedule.iterrows():
			event_name = event['EventName']
			event_round = event['RoundNumber']
			logging.info(f"Processing Event: {year} - Round {event_round} - {event_name}")

			for session_name in SESSIONS_TO_GET:
				logging.debug(f"Attempting Session: {session_name}")
				session_identifier = f"{year}_{event_round:02d}_{event_name}_{session_name}" # Unique ID for logging/paths
				session_save_path = os.path.join(SAVE_DIR, str(year), f"{event_round:02d}_{event_name}", session_name)

				# # Check if all expected CSVs exist for this session, skip if so
				# expected_files = ['laps.csv', 'results.csv', 'weather.csv', 'messages.csv']
				# csvs_exist = all(os.path.exists(os.path.join(session_save_path, fname)) for fname in expected_files)

				# # Check for at least one telemetry file (since driver list may change)
				# telemetry_files_exist = any(
				#     fname.startswith('telemetry_') and fname.endswith('.csv')
				#     for fname in os.listdir(session_save_path) if os.path.isdir(session_save_path)
				# ) if os.path.isdir(session_save_path) else False

				# if csvs_exist and telemetry_files_exist:
				#     logging.info(f"All CSVs already exist for {session_identifier}, skipping session.")
				#     continue
				try:
					session = fastf1.get_session(year, event_name, session_name)

					session.load(laps=True, weather=True, messages=True, telemetry=True)
					logging.info(f"Loaded basic data for {session_identifier}")

					os.makedirs(session_save_path, exist_ok=True)

					if hasattr(session, 'laps') and not session.laps.empty:
						session.laps.to_csv(os.path.join(session_save_path, 'laps.csv'), index=False)
						logging.debug(f"Saved laps for {session_identifier}")

						logging.info(f"Loading telemetry for {session_identifier}...")
						telemetry_saved = False
						for drv_id in session.drivers: # Iterate through driver numbers
							try:
								drv_laps = session.laps.pick_drivers(drv_id)
								if not drv_laps.empty:
									drv_abbr = drv_laps['Driver'].iloc[0]
									drv_tel = drv_laps.get_telemetry()

									if not drv_tel.empty:
										drv_tel = drv_tel.merge(drv_laps[['LapNumber', 'SessionTime']], on='SessionTime', how='left')

										drv_tel.to_csv(os.path.join(session_save_path, f'telemetry_{drv_abbr}.csv'), index=False)
										logging.debug(f"Saved telemetry for driver {drv_abbr} in {session_identifier}")
										telemetry_saved = True
									else:
										logging.debug(f"No telemetry data returned for driver {drv_abbr} in {session_identifier}")

							except Exception as tel_ex:
								logging.warning(f"Could not get/save telemetry for driver {drv_id} in {session_identifier}: {tel_ex}")
						if telemetry_saved:
							logging.info(f"Finished processing telemetry for {session_identifier}")
						else:
							logging.info(f"No telemetry saved for any driver in {session_identifier}")


					if hasattr(session, 'results') and not session.results.empty:
						session.results.to_csv(os.path.join(session_save_path, 'results.csv'), index=False)
						logging.debug(f"Saved results for {session_identifier}")

					if hasattr(session, 'weather_data') and not session.weather_data.empty:
						session.weather_data.to_csv(os.path.join(session_save_path, 'weather.csv'), index=False)
						logging.debug(f"Saved weather for {session_identifier}")

					if hasattr(session, 'messages') and not session.messages.empty:
						session.messages.to_csv(os.path.join(session_save_path, 'messages.csv'), index=False)
						logging.debug(f"Saved messages for {session_identifier}")

					logging.info(f"Successfully processed and saved data for {session_identifier}")

				# except fastf1.core.SessionNotAvailableError:
				#      logging.warning(f"Session {session_name} not available or does not exist for {year} {event_name}. Skipping.")
				except fastf1.core.DataNotLoadedError as e:
					logging.error(f"Data not loaded for {session_identifier}. Might be too recent or unavailable. Error: {e}")
				except ConnectionError as e:
					logging.error(f"Connection error during {session_identifier}: {e}. Check network.")
					time.sleep(10) # Longer sleep on connection error
				except Exception as e:
					# Catch other potential errors (API issues, unexpected data format, etc.)
					logging.error(f"An unexpected error occurred for {session_identifier}: {e.__class__.__name__} - {e}")

				finally:
					# Add a delay after processing each session regardless of success/failure
					logging.debug(f"Waiting {DELAY_SECONDS} seconds before next session...")
					time.sleep(DELAY_SECONDS)

		logging.info(f"--- Finished Processing Year: {year} ---")

	logging.info("--- All Years Processed ---")
"""


with DAG(
	'FastF1_ETL',
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    },
    description='Gets data for a selected weekend from the FastF1 API',
	schedule=None,
	catchup=False,
	start_date=datetime(2025, 1, 1), # Has to be earlier than the logical/execution_date of the POST request.
) as dag:
    get_data_for_weekend_task = PythonOperator(
		task_id="get_data_for_weekend_task",
		python_callable=get_data_for_weekend_callable,
		# provide_context=True,
	)
