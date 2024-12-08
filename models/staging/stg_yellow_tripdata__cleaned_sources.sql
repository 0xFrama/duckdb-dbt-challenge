
with raw_data as (

    select * from 'data/raw/yellow_tripdata_2024-01.parquet' 

),

filtered as (

    select   *
    from    raw_data
    where   
                trip_distance > 0.0
        and     fare_amount >= 0  
        and     total_amount > 0.0
        and     passenger_count > 0.0
        and     tpep_pickup_datetime < tpep_dropoff_datetime     
        and     tip_amount >= 0.0     
        and     tolls_amount >= 0.0   
        and     Airport_fee >= 0.0      
),

renamed as (

    select  cast(VendorID as int) as VendorID,
            cast(tpep_pickup_datetime as TIMESTAMP) as pickup_timestamp,
            cast(tpep_dropoff_datetime as TIMESTAMP) as droppoff_timestamp,
            extract(minute from (droppoff_timestamp - pickup_timestamp))::int as trip_duration_min,
            passenger_count,
            trip_distance,
            RatecodeID,
            store_and_fwd_flag,
            PULocationID,
            DOLocationID,
            payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            Airport_fee
    from    filtered
),

enriched as (

    select  *,
            case 
                when payment_type = 2 then TRUE 
                else FALSE 
            end as is_prepaid,
            case
                when trip_distance < 1.0 then 'short'
                when trip_distance between 1.0 and 5.0 then 'medium'
                else 'long'
            end as trip_distance_category
    from renamed

)

select * from enriched