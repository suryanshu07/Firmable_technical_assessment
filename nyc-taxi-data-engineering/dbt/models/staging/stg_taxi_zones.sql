{{ config(materialized='view', tags=['staging', 'stg_taxi_zones']) }}

SELECT
    locationid        AS location_id,
    borough           AS borough,
    zone              AS zone,
    service_zone      AS service_zone
FROM {{ ref('taxi_zone_lookup') }}