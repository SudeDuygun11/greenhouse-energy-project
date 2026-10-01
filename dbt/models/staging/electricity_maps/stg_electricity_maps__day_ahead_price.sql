with source as (

    select * from {{ source('electricity_maps', 'day_ahead_price') }}

)

select
    datetime as hour_utc,
    coalesce(cast(value as float64), value__v_double) as day_ahead_price_eur_per_mwh,
    _dlt_load_id

from source
