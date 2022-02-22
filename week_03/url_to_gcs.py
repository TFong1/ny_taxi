
"""
    Upload green taxi data to Google Cloud Storage
"""

import io
from mimetypes import init
import os
from time import monotonic
from numpy import object_
import requests
import pandas as pd
import pyarrow
from google.cloud import storage


"""
    Prerequisites:
    1. `pip install pandas pyarrow google-cloud-storage
    2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
    3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""


init_url = "https://nyc-tlc.s3.amazonaws.com/trip+data/"
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc_data_lake_dtc-de-course-339918")


def upload_to_gcs(bucket, object_name, local_file):
    """
    reference:  https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """

    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(12):
        month = "0" + str(i+1)
        month = month[-2:]
        filename = service + "_tripdata_" + year + "-" + month + ".csv"
        request_url = init_url + filename
        r = requests.get(request_url)
        pd.DataFrame(io.StringIO(r.text)).to_csv(filename)
        print(f"Local: {filename}")
        df = pd.read_csv(filename)
        filename = filename.replace(".csv", ".parquet")
        df.to_parquet(filename, engine="pyarrow")
        print(f"Parquet: {filename}")
        upload_to_gcs(BUCKET, f"raw/{service}_tripdata/{year}/{filename}", filename)
        print(f"GCS: {service}/{year}/{filename}")



web_to_gcs("2019", "green")
web_to_gcs("2020", "green")
