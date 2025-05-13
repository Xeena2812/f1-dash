from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
import os

ERGAST_FOLDER = "/opt/airflow/ergast"
TMP_FOLDER = "/opt/airflow/tmp"
ERGAST_DW_CONN = {{ var.value.ergast_dw_postgres }}

default_args = {
	"depends_on_past" : False,
	"retries": 1,
	"retry_delay": timedelta(seconds=30),
}

def load_and_process_csv():
	os.makedirs(TMP_FOLDER, exist_ok=True)
	for filename in os.listdir(ERGAST_FOLDER):
		if filename.endswith('.csv'):
			df = pd.read_csv(os.path.join(ERGAST_FOLDER, filename))
			df.to_parquet(os.path.join(TMP_FOLDER, f"{filename}.parquet"))


def load_into_db():
    pass

with DAG(
	'Ergast ETL jobs',
	default_args=default_args,
	description='ETL jobs for the Ergast dataset.',
) as dag:
    t1 = PythonOperator(
		task_id="load and process csv",
		depends_on_past=False,
		python_callable=load_and_process_csv,
	)