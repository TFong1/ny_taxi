
/*
    BigQuery queries used to support and answer questions for Week 3 homework
*/

-- Create and populate table from FHV parquet files uploaded from Week 2's airflow process
CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.external_fhv_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_data_lake_dtc-de-course-339918/raw/fhv_tripdata/fhv_tripdata_2019-*.parquet']
);

-- Question #1
SELECT COUNT(*) FROM `trips_data_all.external_fhv_tripdata`;

-- Question #2
SELECT COUNT(DISTINCT(dispatching_base_num)) FROM `dtc-de-course-339918.trips_data_all.external_fhv_tripdata`;

-- Question #3
CREATE OR REPLACE TABLE `trips_data_all.fhv_tripdata_partitioned_clustered`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS
SELECT * FROM `dtc-de-course-339918.trips_data_all.external_fhv_tripdata`;

-- Question #4
SELECT COUNT(*) FROM `dtc-de-course-339918.trips_data_all.fhv_tripdata_partitioned_clustered`
WHERE dropoff_datetime BETWEEN '2019-01-01' AND '2019-03-31'
    AND dispatching_base_num IN ('B00987', 'B02060', 'B02279');

