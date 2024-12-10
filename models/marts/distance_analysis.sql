{{ config({"materialized": "table"}) }}

with aggregated_data AS (
    SELECT
            trip_distance_category as segment_trips,
            round(avg(trip_duration_min), 2) as avg_trip_duration,
            {{ sum_quantity(total_amount) }} as total_revenue
    from {{ ref('stg_yellow_tripdata__cleaned_sources') }}
    GROUP BY trip_distance_category 
)

select * from aggregated_data