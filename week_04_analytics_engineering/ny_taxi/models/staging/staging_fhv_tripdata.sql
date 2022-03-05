

{{ config(materialized='view') }}

WITH tripdata AS (
    SELECT
        *,
        row_number() OVER(PARTITION BY dispatching_base_num, pickup_datetime) AS rn
    FROM {{ source('staging', 'external_fhv_tripdata') }}
    WHERE dispatching_base_num IS NOT NULL

)
SELECT
    -- identifiers
    dispatching_base_num,
    CAST(PULocationID AS integer) AS pickup_locationid,
    CAST(DOLocationID as integer) AS dropoff_locationid,
    CAST(SR_Flag as integer) AS SR_Flag,

    -- timestamps
    CAST(pickup_datetime AS timestamp) AS pickup_datetime,
    CAST(dropoff_datetime AS timestamp) AS dropoff_datetime,
FROM tripdata
WHERE rn = 1

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

    LIMIT 100

{% endif %}