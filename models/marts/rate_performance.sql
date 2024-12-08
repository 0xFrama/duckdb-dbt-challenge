{{ config({"materialized": "table"}) }}

with tips as (

    select  *, 
            round((tip_amount / total_amount)*100, 1) as tip_percentange
    from {{ ref('stg_yellow_tripdata__cleaned_sources') }}
    where tip_amount > 0.0
), 

rate_perf as (

    select  VendorID,
            round(AVG(tip_percentange), 1) as avg_tip_percentange
    from tips
    group by VendorID

)

select * from rate_perf