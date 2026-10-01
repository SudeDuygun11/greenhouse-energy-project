with source as (

    select * from {{ source('open_meteo', 'weather') }}

)

select
    datetime as hour_utc,
    temperature_2m as temperature_c,
    relative_humidity_2m as relative_humidity_pct,
    precipitation as precipitation_mm,
    wind_speed_10m as wind_speed_kmh,
    cloud_cover as cloud_cover_pct,
    shortwave_radiation as shortwave_radiation_w_per_m2,
    _dlt_load_id

from source
