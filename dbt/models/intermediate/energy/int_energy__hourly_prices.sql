-- Electricity and gas prices on one hourly grain. Gas has one price per gas day
-- (06:00-06:00 local), so every hour inside that window gets the same gas price.

with hours as (

    select * from {{ ref('int_calendar__hours') }}

),

electricity as (

    select * from {{ ref('stg_energyzero__electricity_prices') }}

),

gas as (

    select * from {{ ref('stg_energyzero__gas_prices') }}

)

select
    hours.hour_utc,
    hours.hour_local,
    hours.date_local,
    hours.hour_of_day_local,
    electricity.electricity_price_eur_per_kwh,
    gas.gas_day,
    gas.gas_price_eur_per_m3

from hours
left join electricity
    on electricity.hour_utc = hours.hour_utc
left join gas
    on hours.hour_utc >= gas.valid_from_utc
    and hours.hour_utc < gas.valid_to_utc
