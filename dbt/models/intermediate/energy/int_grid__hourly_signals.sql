-- All Electricity Maps signals for NL on one hourly row. Carbon intensity has full
-- coverage since 2015, so it drives the grain; the other signals are left-joined.

with carbon_intensity as (

    select * from {{ ref('stg_electricity_maps__carbon_intensity') }}

),

renewable as (

    select * from {{ ref('stg_electricity_maps__renewable_percentage') }}

),

carbon_free as (

    select * from {{ ref('stg_electricity_maps__carbon_free_percentage') }}

),

total_load as (

    select * from {{ ref('stg_electricity_maps__total_load') }}

),

day_ahead as (

    select * from {{ ref('stg_electricity_maps__day_ahead_price') }}

),

mix as (

    select * from {{ ref('stg_electricity_maps__electricity_mix') }}

)

select
    carbon_intensity.hour_utc,
    carbon_intensity.carbon_intensity_gco2eq_per_kwh,
    carbon_intensity.is_estimated as is_carbon_intensity_estimated,
    renewable.renewable_pct,
    carbon_free.carbon_free_pct,
    total_load.total_load_mw,
    day_ahead.day_ahead_price_eur_per_mwh,
    mix.wind_mw,
    mix.solar_mw,
    mix.gas_mw,
    mix.coal_mw,
    mix.nuclear_mw,
    mix.biomass_mw,
    mix.hydro_mw,
    mix.unknown_mw,
    mix.imports_mw,
    mix.exports_mw

from carbon_intensity
left join renewable using (hour_utc)
left join carbon_free using (hour_utc)
left join total_load using (hour_utc)
left join day_ahead using (hour_utc)
left join mix using (hour_utc)
