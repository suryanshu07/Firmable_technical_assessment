{{ config(materialized='table', tags=['marts', 'agg_zone_performance']) }}

WITH base AS (

    SELECT
        pickup_zone,
        pickup_borough,
        pickup_year,
        pickup_month,

        COUNT(*)            AS total_trips,
        AVG(trip_distance)  AS avg_trip_distance,
        AVG(fare_amount)    AS avg_fare,
        SUM(total_amount)   AS total_revenue

    FROM {{ ref('int_trips_enriched') }}

    GROUP BY 1,2,3,4

),

ranked AS (

    SELECT
        *,

        -- ✅ BUSINESS DECISION:
        -- We rank zones within each month instead of globally.
        -- This allows fair comparison across zones in the same time period,
        -- avoids bias due to seasonality or different month lengths,
        -- and provides actionable insights for operations (e.g., top zones this month).

        RANK() OVER (
            PARTITION BY pickup_year, pickup_month
            ORDER BY total_revenue DESC
        ) AS revenue_rank,

        CASE 
            WHEN total_trips > 10000 THEN TRUE
            ELSE FALSE
        END AS is_high_volume_zone

    FROM base

)

SELECT * FROM ranked