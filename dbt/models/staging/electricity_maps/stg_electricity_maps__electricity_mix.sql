-- Generation per source in MW. Wind, coal, nuclear and solar arrive split across an
-- integer column and a `__v_double` variant column, so each pair is coalesced.

with source as (

    select * from {{ source('electricity_maps', 'electricity_mix') }}

)

select
    datetime as hour_utc,
    coalesce(cast(mix__wind as float64), mix__wind__v_double) as wind_mw,
    coalesce(cast(mix__solar as float64), mix__solar__v_double) as solar_mw,
    cast(mix__hydro as float64) as hydro_mw,
    mix__biomass as biomass_mw,
    coalesce(cast(mix__nuclear as float64), mix__nuclear__v_double) as nuclear_mw,
    mix__gas as gas_mw,
    coalesce(cast(mix__coal as float64), mix__coal__v_double) as coal_mw,
    mix__unknown as unknown_mw,
    cast(mix__battery_storage__charge as float64) as battery_storage_charge_mw,
    cast(mix__hydro_storage__charge as float64) as hydro_storage_charge_mw,
    cast(mix__flows__imports as float64) as imports_mw,
    cast(mix__flows__exports as float64) as exports_mw,
    estimation_method,
    _dlt_load_id

from source
