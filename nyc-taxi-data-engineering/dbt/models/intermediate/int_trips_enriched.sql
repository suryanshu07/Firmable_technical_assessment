{{ config(materialized='view', tags=['intermediate', 'int_trips_enriched']) }}

WITH trips AS (

    SELECT * 
    FROM {{ ref('stg_yellow_trips') }}

),

zones AS (

    SELECT * 
    FROM {{ ref('stg_taxi_zones') }}

),

enriched AS (

    SELECT
        -- identifiers
        t.vendor_id,

        -- timestamps
        t.pickup_datetime,
        t.dropoff_datetime,

        -- time features (very useful later)
        EXTRACT(YEAR FROM t.pickup_datetime)  AS pickup_year,
        EXTRACT(MONTH FROM t.pickup_datetime) AS pickup_month,
        EXTRACT(HOUR FROM t.pickup_datetime)  AS pickup_hour,

        -- trip metrics
        t.passenger_count,
        t.trip_distance,
        t.trip_duration_minutes,
        t.fare_amount,
        t.tip_amount,
        t.total_amount,

        -- pickup location enrichment
        t.pickup_location_id,
        zp.borough      AS pickup_borough,
        zp.zone         AS pickup_zone,

        -- dropoff location enrichment
        t.dropoff_location_id,
        zd.borough      AS dropoff_borough,
        zd.zone         AS dropoff_zone

    FROM trips t

    LEFT JOIN zones zp
        ON t.pickup_location_id = zp.location_id

    LEFT JOIN zones zd
        ON t.dropoff_location_id = zd.location_id

),

filtered AS (

    SELECT *
    FROM enriched
    WHERE 
        -- remove invalid trips
        trip_distance > 0
        AND fare_amount > 0
        AND passenger_count > 0

        -- duration sanity check
        AND trip_duration_minutes BETWEEN 1 AND 180

)

SELECT * FROM filtered