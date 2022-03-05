"""
 Data Talks Club Week 2 Homework
 Ingest Zones
"""

import os
import logging


from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET_ID = os.environ.get("GCP_GCS_BUCKET")

dataset_file = "taxi+_zone_lookup.csv"
dataset_url = f"https://s3.amazonaws.com/nyc-tlc/misc/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace(".csv", ".parquet")


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



default_args = dict(
    owner = "airflow",
    start_date = days_ago(1),
    depends_on_past = False,
    retries = 1
)


with DAG(
    dag_id = "ingest_zones_gcs",
    schedule_interval = "@once",
    default_args = default_args,
    catchup = False,
    max_active_runs = 1,
    tags = ["dtc-de"]
) as dag:


    download_dataset_task = BashOperator(
        task_id = "download_dataset",
        bash_command = "curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    format_to_parquet_task = PythonOperator(
        task_id = "format_to_parquet",
        python_callable = format_to_parquet,
        op_kwargs = dict(
            src_file = f"{path_to_local_home}/{dataset_file}"
        )
    )

    local_to_gcs_task = PythonOperator(
        task_id = "local_to_gcs",
        python_callable = upload_to_gcs,
        op_kwargs = dict(
            bucket = BUCKET_ID,
            object_name = f"raw/zones/{parquet_file}",
            local_file = f"{path_to_local_home}/{parquet_file}"
        )
    )


    # Clean up local files
    remove_local_files_task = BashOperator(
        task_id = "remove_local_csv_file",
        bash_command = f"rm {path_to_local_home}/{dataset_file} {path_to_local_home}/{parquet_file}"
    )


    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> remove_local_files_task