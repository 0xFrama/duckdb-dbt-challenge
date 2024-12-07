WITH trips_with_time_slots AS (
    SELECT
        *,
        MAKE_TIME(
            EXTRACT(HOUR FROM pickup_time)::INT,
            EXTRACT(MINUTE FROM pickup_time)::INT,
            EXTRACT(second FROM pickup_time)::INT
        ) AS pickup_time_new
    FROM {{ ref('stg_yellow_tripdata__cleaned_sources') }}
),

classified_trips AS (
    SELECT
        *,
        CASE
            WHEN pickup_time_new BETWEEN TIME '05:00:00' AND TIME '11:59:59' THEN 'Morning'
            WHEN pickup_time_new BETWEEN TIME '12:00:00' AND TIME '16:59:59' THEN 'Afternoon'
            WHEN pickup_time_new BETWEEN TIME '17:00:00' AND TIME '21:59:59' THEN 'Evening'
            ELSE 'Night'
        END AS time_of_day
    FROM trips_with_time_slots
),

aggregated_data AS (
    SELECT
        time_of_day,
        COUNT(*) AS total_trips,
        round(SUM(total_amount), 2) AS total_revenue
    FROM classified_trips
    GROUP BY time_of_day
)

SELECT *
FROM aggregated_data
ORDER BY 
    CASE time_of_day
        WHEN 'Morning' THEN 1
        WHEN 'Afternoon' THEN 2
        WHEN 'Evening' THEN 3
        WHEN 'Night' THEN 4
    END
