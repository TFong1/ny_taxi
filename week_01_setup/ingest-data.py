#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.user
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = "output.csv"

    os.system(f"curl -o {csv_name} {url}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
    df.to_sql(name=table_name, con=engine, if_exists="append")

    while True:
        t_start = time()

        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists="append")

        t_end = time()

        print("Inserted another chunk, took %.3f seconds." % (t_end - t_start))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres.")

    parser.add_argument("--user", required=True, help="User name for postgres.")
    parser.add_argument("--password", required=True, help="Password for postgres.")
    parser.add_argument("--host", required=True, help="Host name(or IP address) for postgres.")
    parser.add_argument("--port", required=True, help="Port number for postgres.")
    parser.add_argument("--db", required=True, help="Database name for postgres.")
    parser.add_argument("--table_name", required=True, help="Name of table where we will write the results to for postgres.")
    parser.add_argument("--url", required=True, help="URL of the CSV file.")

    args = parser.parse_args()

    main(args)    
