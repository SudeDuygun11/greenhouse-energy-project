with source as (

    select * from {{ source('energyzero', 'electricity_prices') }}

)

select
    timestamp_utc as hour_utc,
    datetime(timestamp_utc, '{{ var("local_timezone") }}') as hour_local,
    price_eur_per_kwh as electricity_price_eur_per_kwh,
    _dlt_load_id

from source
