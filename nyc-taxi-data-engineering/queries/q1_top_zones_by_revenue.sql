-- Q1: Top 10 pickup zones by revenue in each month of 2023
-- Strategy:
-- - Aggregate at (month, zone) level to reduce data early
-- - Use window function (RANK) partitioned by month
-- - In Snowflake: clustering on pickup_month improves pruning

WITH monthly_zone_revenue AS (
    SELECT
        DATE_TRUNC('month', pickup_datetime) AS month,
        pickup_zone,
        SUM(total_amount) AS total_revenue
    FROM fct_trips
    WHERE pickup_datetime >= '2023-01-01'
      AND pickup_datetime < '2024-01-01'
    GROUP BY 1, 2
),

ranked AS (
    SELECT
        *,
        RANK() OVER (
            PARTITION BY month
            ORDER BY total_revenue DESC
        ) AS revenue_rank
    FROM monthly_zone_revenue
)

SELECT *
FROM ranked
WHERE revenue_rank <= 10
ORDER BY month, revenue_rank;