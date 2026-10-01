with source as (

    select * from {{ source('electricity_maps', 'total_load') }}

)

select
    datetime as hour_utc,
    value as total_load_mw,
    is_estimated,
    _dlt_load_id

from source
