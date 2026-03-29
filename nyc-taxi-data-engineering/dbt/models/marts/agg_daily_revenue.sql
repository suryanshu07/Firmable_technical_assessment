{{ config(materialized='table', tags=['marts', 'agg_daily_revenue']) }}

SELECT
    DATE(pickup_datetime) AS trip_date,

    COUNT(*)                        AS total_trips,
    SUM(total_amount)              AS total_revenue,
    AVG(fare_amount)               AS avg_fare,
    SUM(tip_amount)                AS total_tips,

    -- tip rate %
    CASE 
        WHEN SUM(total_amount) > 0 
        THEN (SUM(tip_amount) / SUM(total_amount)) * 100
        ELSE 0
    END                            AS tip_rate_percent

FROM {{ ref('int_trips_enriched') }}

GROUP BY 1
ORDER BY 1