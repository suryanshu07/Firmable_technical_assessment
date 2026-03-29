-- Q3: Max gap between consecutive trips (same pickup zone, per day)

WITH ordered_trips AS (
    SELECT
        pickup_zone,
        DATE(pickup_datetime) AS trip_date,
        pickup_datetime,
        dropoff_datetime,

        LAG(dropoff_datetime) OVER (
            PARTITION BY pickup_zone, DATE(pickup_datetime)
            ORDER BY pickup_datetime
        ) AS prev_dropoff_time

    FROM fct_trips
    WHERE pickup_datetime >= '2023-01-01'
      AND pickup_datetime < '2024-01-01'
),

gaps AS (
    SELECT
        pickup_zone,
        trip_date,

        DATEDIFF(
            'minute',
            prev_dropoff_time,
            pickup_datetime
        ) AS gap_minutes

    FROM ordered_trips
    WHERE prev_dropoff_time IS NOT NULL
)

SELECT
    trip_date,
    pickup_zone,
    MAX(gap_minutes) AS max_gap_minutes
FROM gaps
GROUP BY 1, 2
ORDER BY trip_date, max_gap_minutes DESC;

-- ============================================================
-- PERFORMANCE CONSIDERATIONS (SNOWFLAKE)
-- ============================================================
-- This query computes gaps between consecutive trips using LAG(),
-- partitioned by (pickup_zone, trip_date) and ordered by pickup_datetime.
-- On large datasets (~38M rows), this can be expensive due to sorting
-- and window function execution.

-- Optimization strategies:

-- 1. CLUSTERING KEY
-- Cluster the underlying table on:
--   (pickup_zone, DATE(pickup_datetime))
-- This aligns with the window partition keys, improving data locality
-- and reducing sort/shuffle overhead during execution.

-- 2. PARTITION PRUNING
-- The query filters on a bounded date range (year 2023), allowing
-- Snowflake to prune micro-partitions efficiently when data is
-- well-clustered on pickup_datetime.

-- 3. MATERIALIZATION STRATEGY
-- Instead of running directly on the full fact table, create an
-- intermediate table with only required columns:
--   pickup_zone, pickup_datetime, dropoff_datetime
-- This reduces I/O and improves performance of the window function.

-- 4. RESULT CACHING
-- Snowflake automatically caches results for identical queries.
-- Repeated executions (e.g., dashboards) will be near-instant
-- if underlying data has not changed.

-- 5. WAREHOUSE SIZING
-- The query should run within SLA on an X-Small warehouse with
-- proper clustering. For heavy workloads, scaling to Small can
-- improve performance due to parallelism.

-- Overall, aligning clustering keys with window partitions and
-- minimizing scanned data are the most impactful optimizations.
-- ============================================================