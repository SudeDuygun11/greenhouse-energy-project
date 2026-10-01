with source as (

    select * from {{ source('electricity_maps', 'renewable_percentage') }}

)

select
    datetime as hour_utc,
    cast(value as float64) as renewable_pct,
    is_estimated,
    _dlt_load_id

from source
