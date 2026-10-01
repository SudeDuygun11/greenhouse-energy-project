with source as (

    select * from {{ source('electricity_maps', 'carbon_free_percentage') }}

)

select
    datetime as hour_utc,
    cast(value as float64) as carbon_free_pct,
    is_estimated,
    _dlt_load_id

from source
