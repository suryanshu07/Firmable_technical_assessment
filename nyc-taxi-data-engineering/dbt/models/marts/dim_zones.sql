{{ config(materialized='table', tags=['marts', 'dim_zones']) }}

SELECT DISTINCT
    location_id,
    borough,
    zone,
    service_zone
FROM {{ ref('stg_taxi_zones') }}