-- Market context per hour: what energy cost, how clean the grid was, and the weather.
-- Independent of the greenhouse. Hours with no price and no grid data are dropped
-- (the spine runs ahead of the loaded data).

with prices as (

    select * from {{ ref('int_energy__hourly_prices') }}

),

grid as (

    select * from {{ ref('int_grid__hourly_signals') }}

),

weather as (

    select * from {{ ref('stg_open_meteo__weather') }}

)

select
    prices.hour_utc,
    prices.hour_local,
    prices.date_local,
    prices.hour_of_day_local,

    prices.electricity_price_eur_per_kwh,
    prices.gas_day,
    prices.gas_price_eur_per_m3,
    grid.day_ahead_price_eur_per_mwh,

    grid.carbon_intensity_gco2eq_per_kwh,
    grid.is_carbon_intensity_estimated,
    grid.renewable_pct,
    grid.carbon_free_pct,
    grid.total_load_mw,
    grid.wind_mw,
    grid.solar_mw,
    grid.gas_mw,
    grid.coal_mw,
    grid.nuclear_mw,
    grid.biomass_mw,
    grid.hydro_mw,
    grid.unknown_mw,
    grid.imports_mw,
    grid.exports_mw,

    weather.temperature_c,
    weather.relative_humidity_pct,
    weather.precipitation_mm,
    weather.wind_speed_kmh,
    weather.cloud_cover_pct,
    weather.shortwave_radiation_w_per_m2

from prices
left join grid using (hour_utc)
left join weather using (hour_utc)
where prices.electricity_price_eur_per_kwh is not null
    or grid.hour_utc is not null
