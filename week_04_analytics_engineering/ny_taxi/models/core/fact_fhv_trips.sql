

{{ config(materialized='table') }}

WITH fhv_tripdata AS (
    SELECT
        *,
        'Fhv' AS service_type
    FROM {{ ref('staging_fhv_tripdata') }}
),

dim_zones AS (
    SELECT * FROM {{ ref('dim_zones') }}
    WHERE borough != 'Unknown'
)

SELECT
    fhv_tripdata.dispatching_base_num,
    fhv_tripdata.service_type,
    fhv_tripdata.pickup_locationid,
    pickup_zone.zone AS pickup_zone,
    pickup_zone.borough AS pickup_borough,
    fhv_tripdata.dropoff_locationid,
    dropoff_zone.zone AS dropoff_zone,
    dropoff_zone.borough AS dropoff_borough,
    fhv_tripdata.pickup_datetime,
    fhv_tripdata.dropoff_datetime,
    fhv_tripdata.SR_Flag

FROM fhv_tripdata
    INNER JOIN dim_zones AS pickup_zone 
        ON fhv_tripdata.pickup_locationid = pickup_zone.locationid
    INNER JOIN dim_zones AS dropoff_zone
        ON fhv_tripdata.dropoff_locationid = dropoff_zone.locationid
