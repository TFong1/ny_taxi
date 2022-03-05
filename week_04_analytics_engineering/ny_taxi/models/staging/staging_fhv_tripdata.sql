

{{ config(materialized='view') }}


SELECT
    -- identifiers
    dispatching_base_num,
    CAST(PULocationID AS integer) AS pickup_locationid,
    CAST(DOLocationID as integer) AS dropoff_locationid,
    SR_Flag,

    -- timestamps
    CAST(pickup_datetime AS timestamp) AS pickup_datetime,
    CAST(dropoff_datetime AS timestamp) AS dropoff_datetime,
FROM {{ source('staging', 'external_fhv_tripdata') }}


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

    LIMIT 100

{% endif %}