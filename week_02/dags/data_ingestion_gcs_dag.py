"""
 Data Talks Club Week 2 Homework
 Ingest Yellow Taxi Data for 2019 and 2020
"""

import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

from google.cloud import storage
from numpy import datetime_as_string
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET_ID = os.environ.get("GCP_GCS_BUCKET")
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


def format_to_parquet(src_file):
    if not src_file.endswith(".csv"):
        logging.error("Can only accept source files in CSV format.")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace(".csv", ".parquet"))


# NOTE:  This takes a long time, but faster if Internet has better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
        Reference:  https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
        :param bucket: GCS bucket name
        :param object_name: target path & filename
        :param local_file: source path & filename
        :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Reference:  https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # END WORKAROUND

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def upload_parquetized_data(
    dag,
    dataset_url,
    local_csv_path,
    local_parquet_path,
    gcs_parquet_path
    ):
    with dag:

        download_dataset_task = BashOperator(
            task_id = "download_dataset",
            # include an "f" option for curl because it will fail if file not found; we need a "red" indicator to let us know no file was found
            bash_command = f"curl -sSLf {dataset_url} > {local_csv_path}"
        )

        format_to_parquet_task = PythonOperator(
            task_id = "format_to_parquet",
            python_callable = format_to_parquet,
            op_kwargs = dict(
                src_file = f"{local_csv_path}"
            )
        )

        local_to_gcs_task = PythonOperator(
            task_id = "local_to_gcs",
            python_callable = upload_to_gcs,
            op_kwargs = dict(
                bucket = BUCKET_ID,
                object_name = f"{gcs_parquet_path}",
                local_file = f"{local_parquet_path}"
            )
        )

        
        # Clean up local files
        remove_local_files_task = BashOperator(
            task_id = "remove_local_csv_file",
            bash_command = f"rm {local_csv_path} {local_parquet_path}"
        )


        download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> remove_local_files_task


default_args = dict(
    owner = "airflow",
    start_date = days_ago(1),
    depends_on_past = False,
    retries = 1
)


# https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-01.csv
yellow_taxi_data_dag = DAG(
    dag_id = "ingest_yellow_taxi_gcs",
    schedule_interval = "0 6 2 * *",
    start_date = datetime(2019, 1, 1),
    end_date = datetime(2021, 1, 1),
    default_args = default_args,
    catchup = True,
    max_active_runs = 1,
    tags = ["dtc-de"]
)

yellow_taxi_dataset_file = "yellow_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv"
yellow_taxi_dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{yellow_taxi_dataset_file}"
yellow_taxi_csv_path = f"{path_to_local_home}/{yellow_taxi_dataset_file}"
yellow_taxi_parquet_file = yellow_taxi_dataset_file.replace('.csv', '.parquet')
yellow_taxi_parquet_path = f"{path_to_local_home}/{yellow_taxi_parquet_file}"
yellow_taxi_gcs_parquet_path = f"raw/yellow_taxi_tripdata/{{ execution_date.strftime('%Y') }}/{yellow_taxi_parquet_file}"

upload_parquetized_data(
    dag = yellow_taxi_data_dag,
    dataset_url= yellow_taxi_dataset_url,
    local_csv_path= yellow_taxi_csv_path,
    local_parquet_path= yellow_taxi_parquet_path,
    gcs_parquet_path= yellow_taxi_gcs_parquet_path
)


# https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2019-01.csv
green_taxi_data_dag = DAG(
    dag_id = "ingest_green_taxi_gcs",
    schedule_interval = "0 6 2 * *",
    start_date = datetime(2019, 1, 1),
    end_date = datetime(2021, 1, 1),
    default_args = default_args,
    catchup = True,
    max_active_runs = 1,
    tags = ["dtc-de"]
)

green_taxi_dataset_file = "green_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv"
green_taxi_dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{green_taxi_dataset_file}"
green_taxi_csv_path = f"{path_to_local_home}/{green_taxi_dataset_file}"
green_taxi_parquet_file = green_taxi_dataset_file.replace('.csv', '.parquet')
green_taxi_parquet_path = f"{path_to_local_home}/{green_taxi_parquet_file}"
green_taxi_gcs_parquet_path = "raw/green_taxi_tripdata/{{ execution_date.strftime('%Y') }}/" + f"{green_taxi_parquet_file}"

upload_parquetized_data(
    dag = green_taxi_data_dag,
    dataset_url= green_taxi_dataset_url,
    local_csv_path= green_taxi_csv_path,
    local_parquet_path= green_taxi_parquet_path,
    gcs_parquet_path= green_taxi_gcs_parquet_path
)


fhv_taxi_data_dag = DAG(
    dag_id = "ingest_fhv_taxi_gcs",
    schedule_interval="0 6 2 * *",
    start_date= datetime(2019, 1, 1),
    end_date= datetime(2020, 1, 1),
    default_args= default_args,
    catchup= True,
    max_active_runs= 1,
    tags= ["dtc-de"]
)

# https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2019-_mm_.csv
fhv_taxi_dataset_file = "fhv_tripdata_{{ execution_date.strftime('%Y-%m') }}.csv"
fhv_taxi_dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{fhv_taxi_dataset_file}"
fhv_taxi_csv_path = f"{path_to_local_home}/{fhv_taxi_dataset_file}"
fhv_taxi_parquet_file = fhv_taxi_dataset_file.replace('.csv', '.parquet')
fhv_taxi_parquet_path = f"{path_to_local_home}/{fhv_taxi_parquet_file}"
fhv_taxi_gcs_parquet_path = f"raw/fhv_tripdata/{{ execution_date.strftime('%Y') }}/{fhv_taxi_parquet_file}"

upload_parquetized_data(
    dag = fhv_taxi_data_dag,
    dataset_url= fhv_taxi_dataset_url,
    local_csv_path= fhv_taxi_csv_path,
    local_parquet_path= fhv_taxi_parquet_path,
    gcs_parquet_path= fhv_taxi_gcs_parquet_path
)



# https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
zone_data_dag = DAG(
    dag_id = "ingest_zone_gcs",
    schedule_interval="@once",
    default_args= default_args,
    catchup= False,
    max_active_runs= 1,
    tags= ["dtc-de"]
)

zone_dataset_file = "taxi+_zone_lookup.csv"
zone_dataset_url = f"https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
zone_csv_path = f"{path_to_local_home}/{zone_dataset_file}"
zone_parquet_file = zone_dataset_file.replace('.csv', '.parquet')
zone_parquet_path = f"{path_to_local_home}/{zone_parquet_file}"
zone_gcs_parquet_path = f"raw/zone/{zone_parquet_file}"

upload_parquetized_data(
    dag = zone_data_dag,
    dataset_url= zone_dataset_url,
    local_csv_path= zone_csv_path,
    local_parquet_path= zone_parquet_path,
    gcs_parquet_path= zone_gcs_parquet_path
)

