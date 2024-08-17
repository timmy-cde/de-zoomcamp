{{
    config(
        materialized='view'
    )
}}

select
    dispatching_base_num,

    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pulocation_id,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dolocation_id,
    {{ dbt.safe_cast("sr_flag", api.Column.translate_type("integer")) }} as sr_flag,
    
    affiliated_base_number

from {{ source('staging', 'fhv_2019_2020_bq') }}
where extract(YEAR FROM pickup_date) = 2019

-- {% if var('is_test_run', default=true) %}

--     limit 100

-- {% endif %}