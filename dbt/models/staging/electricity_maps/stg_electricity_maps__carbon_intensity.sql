with source as (

    select * from {{ source('electricity_maps', 'carbon_intensity') }}

)

select
    datetime as hour_utc,
    cast(value as float64) as carbon_intensity_gco2eq_per_kwh,
    is_estimated,
    _dlt_load_id

from source
