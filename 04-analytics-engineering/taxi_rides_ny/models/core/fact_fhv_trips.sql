{{
    config(
        materialized='table'
    )
}}

with fhv_trip_data as (
    select *,
        'fhv' as service_type
    from {{ ref('stg_fhv_2019_bq') }}
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select 
    fhv_trip_data.dispatching_base_num,
    fhv_trip_data.service_type,
    fhv_trip_data.pickup_datetime,
    fhv_trip_data.pulocation_id as pickup_locationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    fhv_trip_data.dropoff_datetime,
    fhv_trip_data.dolocation_id as dropoff_locationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    fhv_trip_data.sr_flag,
    fhv_trip_data.affiliated_base_number
from fhv_trip_data
inner join dim_zones as pickup_zone
on fhv_trip_data.pulocation_id = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_trip_data.dolocation_id = dropoff_zone.locationid
