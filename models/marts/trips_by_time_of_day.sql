{{ config({"materialized": "table"}) }}

with trips_with_pickup_time as (
    select
        *,
        make_time(
            extract(hour from pickup_timestamp)::int,
            extract(minute from pickup_timestamp)::int,
            extract(second from pickup_timestamp)::int
        ) as pickup_time
    from {{ ref('stg_yellow_tripdata__cleaned_sources') }}
),

classified_trips as (
    select  *,
            case
                when pickup_time between time '05:00:00' and time '11:59:59' then 'Morning'
                when pickup_time between time '12:00:00' and time '16:59:59' then 'Afternoon'
                when pickup_time between time '17:00:00' and time '21:59:59' then 'Evening'
                else 'Night'
            end as pickup_time_slots
    from trips_with_pickup_time
),

aggregated_data AS (
    SELECT
        pickup_time_slots,
        count(*) as total_trips,
        round(sum(total_amount), 2) as total_revenue
    FROM classified_trips
    GROUP BY pickup_time_slots
)

select  *
from    aggregated_data
order by 
    case pickup_time_slots
        when 'Morning' then 1
        when 'Afternoon' then 2
        when 'Evening' then 3
        when 'Night' then 4
    end

