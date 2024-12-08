{{ config({"materialized": "table"}) }}

with aggregated_data AS (
    SELECT
        PULocationID,
        COUNT(*) AS total_trips,
        round(SUM(total_amount)) AS total_revenue
    from {{ ref('stg_yellow_tripdata__cleaned_sources') }}
    GROUP BY PULocationID 
),

test as (

    select  *
    from aggregated_data
    order by total_trips desc, total_revenue desc
    limit 5
)

select * from test