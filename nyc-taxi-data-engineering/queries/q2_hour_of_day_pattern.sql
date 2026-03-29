-- Q2: Hour-of-day demand pattern

WITH hourly_stats AS (
    SELECT
        EXTRACT(hour FROM pickup_datetime) AS hour_of_day,

        COUNT(*) AS total_trips,

        AVG(fare_amount) AS avg_fare,

        AVG(
            CASE 
                WHEN total_amount > 0 
                THEN tip_amount / total_amount 
                ELSE 0
            END
        ) AS avg_tip_pct

    FROM fct_trips
    GROUP BY 1
)

SELECT
    hour_of_day,
    total_trips,
    avg_fare,
    avg_tip_pct,

    AVG(total_trips) OVER (
        ORDER BY hour_of_day
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3hr_avg_trips

FROM hourly_stats
ORDER BY hour_of_day;