{{ config(
    materialized='view',
    tags=['staging', 'stg_yellow_trips']
) }}

WITH source AS (

    SELECT * 
    FROM {{ source('nyc_taxi', 'yellow_tripdata') }}

),

renamed AS (

    SELECT
        -- identifiers
        vendorid                     AS vendor_id,
        passenger_count             AS passenger_count,

        -- timestamps
        tpep_pickup_datetime        AS pickup_datetime,
        tpep_dropoff_datetime       AS dropoff_datetime,

        -- location
        pulocationid                AS pickup_location_id,
        dolocationid                AS dropoff_location_id,

        -- trip metrics
        trip_distance               AS trip_distance,
        fare_amount                 AS fare_amount,
        extra                       AS extra,
        mta_tax                     AS mta_tax,
        tip_amount                  AS tip_amount,
        tolls_amount                AS tolls_amount,
        improvement_surcharge       AS improvement_surcharge,
        total_amount                AS total_amount,

        -- payment
        payment_type                AS payment_type,

        -- computed
        DATEDIFF(
        'minute',
        tpep_pickup_datetime,
        tpep_dropoff_datetime
    ) AS trip_duration_minutes

    FROM source

)

SELECT * FROM renamed
WHERE pickup_datetime IS NOT NULL