
with raw_data as (

    select * from 'data/raw/yellow_tripdata_2024-01.parquet' 

),

cleaned_data as (

    select   *
    from    raw_data
    where   trip_distance >= 0.0  
    and     total_amount > 0.0    
    and     tip_amount >= 0.0     
    and     tolls_amount >= 0.0   
    and     passenger_count > 0.0
    and     fare_amount >= 0      
    and     tpep_pickup_datetime < tpep_dropoff_datetime
),

formatted_data as (

    select  VendorID,
            cast(tpep_pickup_datetime as TIMESTAMP) as pickup_time,
            cast(tpep_dropoff_datetime as TIMESTAMP) as droppoff_time,
            extract(minute from (droppoff_time - pickup_time)) as trip_duration,
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
    from    cleaned_data
),

enriched_data as (

    select  *,
            case 
                when payment_type = 2.0 then TRUE 
                else FALSE 
            end as is_prepaid,
            case
                when trip_distance < 1.0 then 'short'
                when trip_distance between 1.0 and 5.0 then 'medium'
                else 'long'
            end as trip_distance_category
    from formatted_data

)

select * from enriched_data