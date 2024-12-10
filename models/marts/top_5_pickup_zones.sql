{{ config({"materialized": "table"}) }}

with top_five_zones as (
    select  PULocationID,
            count(*) as number_of_trips,
            {{ sum_quantity(total_amount) }} as total_revenue
    from {{ ref('stg_yellow_tripdata__cleaned_sources') }}
    group by PULocationID
    order by count(*) desc
    limit 5
)

select * from top_five_zones