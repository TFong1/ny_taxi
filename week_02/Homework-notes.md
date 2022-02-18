
# Homework Week #2 Notes

## For-Hire Vehicle (FHV) 2019 trip records

* template: "https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2019-_mm_.csv"; where _mm_ is the two digit month


## Yellow taxi records

* template (2019):  "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-_mm_.csv"; where _mm_ is the two digit month

* template (2020):  "https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2020-_mm_.csv"; where _mm_ is the two digit month

## Zones (lookup table) data

* URL:  "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"

# Useful Information

## Run a bash command inside a container:
    $ docker exec -it f54da8993fd9 bash

where f54da8993fd9 is the _container id or name_.
You can also replace _bash_ with a linux command.

## Backfill Execution

We need to perform a manual backfill for the homework.
[Reference](https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html#:~:text=Just%20run%20the%20command%20-%20airflow%20dags%20trigger,DAG%20can%20be%20specified%20using%20the%20-e%20argument.)

command:

    airflow dags backfill \
        --start-date START_DATE \
        --end-date END_DATE \
        dag_id

# Homework #2 Answers

Question #1:  What should be the start date for this dag?
Answer:  2019-01-01

Question #2:  How often do we need to run this DAG?
Answer:  Monthly

Question #3:  How many DAG runs are green for the data in 2019 (FHV data) after finishing everything?
Answer:  12 (for the 12 months in 2019)

Question #4:  How often does DAG for zone data need to run?
Answer:  Once  (as we assume the data is static for the most part)
