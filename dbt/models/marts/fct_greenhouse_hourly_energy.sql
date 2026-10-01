-- Greenhouse energy use, cost and emissions. Grain: one row per hour per scenario
-- (baseline / optimized). Costs use EnergyZero all-in prices (incl. taxes and VAT).
-- Cost and emission columns are null where the matching price or grid data is missing.

with demand as (

    select * from {{ ref('int_greenhouse__hourly_energy_demand') }}

),

prices as (

    select * from {{ ref('int_energy__hourly_prices') }}

),

grid as (

    select * from {{ ref('int_grid__hourly_signals') }}

)

select
    demand.hour_utc,
    demand.hour_local,
    demand.date_local,
    demand.hour_of_day_local,
    demand.scenario,

    demand.is_lighting_on,
    demand.heating_setpoint_c,
    demand.temperature_c,
    demand.shortwave_radiation_w_per_m2,

    demand.lighting_kwh,
    demand.base_load_kwh,
    demand.electricity_kwh,
    demand.heat_demand_kwh,
    demand.gas_m3,

    prices.electricity_price_eur_per_kwh,
    prices.gas_price_eur_per_m3,
    demand.electricity_kwh * prices.electricity_price_eur_per_kwh as electricity_cost_eur,
    demand.gas_m3 * prices.gas_price_eur_per_m3 as gas_cost_eur,
    demand.electricity_kwh * prices.electricity_price_eur_per_kwh
        + demand.gas_m3 * prices.gas_price_eur_per_m3 as total_energy_cost_eur,

    grid.carbon_intensity_gco2eq_per_kwh,
    demand.electricity_kwh * grid.carbon_intensity_gco2eq_per_kwh / 1000 as electricity_co2_kg,
    demand.gas_m3 * {{ var("gas_kg_co2_per_m3") }} as gas_co2_kg,
    demand.electricity_kwh * grid.carbon_intensity_gco2eq_per_kwh / 1000
        + demand.gas_m3 * {{ var("gas_kg_co2_per_m3") }} as total_co2_kg

from demand
left join prices using (hour_utc)
left join grid using (hour_utc)
