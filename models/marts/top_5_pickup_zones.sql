with top_five_zones as (
    select  PULocationID,
            count(*) as number_of_trips,
            round(sum(total_amount), 2) as total_revenue
    from {{ ref('stg_yellow_tripdata__cleaned_sources') }}
    group by PULocationID
    order by count(*) desc
)

select * from top_five_zones limit 5