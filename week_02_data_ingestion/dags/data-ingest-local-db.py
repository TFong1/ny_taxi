
import os

from threading import local
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from ingest_callable import ingest_callable


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.getenv("PG_HOST")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")


local_ingestion_workflow = DAG(
    "LocalIngestionDAG",
    schedule_interval = "0 6 2 * *",
    start_date = datetime(2021, 1, 1)
)


# Use Jinja templates ({{ }}) to generate the year and month for the data
URL_PATH = "https://s3.amazonaws.com/nyc-tlc/trip+data"
URL_FILENAME = "yellow_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv"
URL_TEMPLATE = URL_PATH + "/" + URL_FILENAME
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + "/output_{{ execution_date.strftime('%Y-%m') }}.csv"

TABLE_NAME_TEMPLATE = "yellow_taxi_{{ execution_date.strftime('%Y_%m') }}"

with local_ingestion_workflow:
    curl_task = BashOperator(
        task_id = "curlfile",
        bash_command = f"curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}"
    )

    ingest_task = PythonOperator(
        task_id = "ingestdata",
        python_callable = ingest_callable,
        op_kwargs = dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME_TEMPLATE,
            csv_file=OUTPUT_FILE_TEMPLATE
        )
    )



    curl_task >> ingest_task