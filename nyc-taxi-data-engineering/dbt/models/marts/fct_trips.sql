{{ config(materialized='table', tags=['marts', 'fct_trips']) }}

SELECT
    -- surrogate key (optional but good practice)
    ROW_NUMBER() OVER () AS trip_id,

    -- identifiers
    vendor_id,

    -- timestamps
    pickup_datetime,
    dropoff_datetime,
    pickup_year,
    pickup_month,
    pickup_hour,

    -- metrics
    passenger_count,
    trip_distance,
    trip_duration_minutes,
    fare_amount,
    total_amount,

    -- locations
    pickup_location_id,
    pickup_borough,
    pickup_zone,
    dropoff_location_id,
    dropoff_borough,
    dropoff_zone

FROM {{ ref('int_trips_enriched') }}