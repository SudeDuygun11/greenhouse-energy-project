-- Hourly electricity and gas demand of the synthetic greenhouse under two scenarios:
--
--   baseline  : lamps follow the fixed monthly schedule (when it is dark enough).
--   optimized : the same number of lamp hours per local day, moved to the cheapest
--               dark hours of that day. Days with a missing electricity price keep
--               the baseline. Simplification: lamp hours need not be contiguous, so
--               a real crop's minimum dark period is not enforced.
--
-- Heating follows a steady-state heat balance per m2 of floor area:
--   heat demand = loss through cover - solar gain - useful lamp heat  (never < 0)

with schedule as (

    select * from {{ ref('int_greenhouse__hourly_schedule') }}

),

weather as (

    select * from {{ ref('stg_open_meteo__weather') }}

),

prices as (

    select * from {{ ref('int_energy__hourly_prices') }}

),

hourly as (

    select
        schedule.hour_utc,
        schedule.hour_local,
        schedule.date_local,
        schedule.hour_of_day_local,
        schedule.heating_setpoint_c,
        weather.temperature_c,
        weather.shortwave_radiation_w_per_m2,
        prices.electricity_price_eur_per_kwh,
        weather.shortwave_radiation_w_per_m2 < {{ var("lighting_radiation_cutoff_w_per_m2") }} as is_dark_enough_for_lighting,
        schedule.is_lighting_scheduled
            and weather.shortwave_radiation_w_per_m2 < {{ var("lighting_radiation_cutoff_w_per_m2") }} as is_lighting_on_baseline
    from schedule
    inner join weather using (hour_utc)
    left join prices using (hour_utc)

),

ranked as (

    select
        *,
        countif(is_lighting_on_baseline) over (partition by date_local) as lighting_hours_in_day,
        countif(electricity_price_eur_per_kwh is null) over (partition by date_local) as unpriced_hours_in_day,
        case
            when is_dark_enough_for_lighting then row_number() over (
                partition by date_local, is_dark_enough_for_lighting
                order by electricity_price_eur_per_kwh, hour_utc
            )
        end as price_rank_among_dark_hours
    from hourly

),

scenarios as (

    select
        ranked.*,
        'baseline' as scenario,
        is_lighting_on_baseline as is_lighting_on
    from ranked

    union all

    select
        ranked.*,
        'optimized' as scenario,
        case
            when unpriced_hours_in_day > 0 then is_lighting_on_baseline
            else coalesce(price_rank_among_dark_hours <= lighting_hours_in_day, false)
        end as is_lighting_on
    from ranked

),

demand as (

    select
        hour_utc,
        hour_local,
        date_local,
        hour_of_day_local,
        scenario,
        is_lighting_on,
        heating_setpoint_c,
        temperature_c,
        shortwave_radiation_w_per_m2,
        if(is_lighting_on, {{ var("lighting_power_w_per_m2") }}, 0) as lighting_w_per_m2,
        greatest(
            0,
            {{ var("heat_loss_w_per_m2_k") }} * (heating_setpoint_c - temperature_c)
            - {{ var("solar_heat_gain_fraction") }} * shortwave_radiation_w_per_m2
            - {{ var("lighting_heat_fraction") }} * if(is_lighting_on, {{ var("lighting_power_w_per_m2") }}, 0)
        ) as heat_demand_w_per_m2
    from scenarios

)

select
    hour_utc,
    hour_local,
    date_local,
    hour_of_day_local,
    scenario,
    is_lighting_on,
    heating_setpoint_c,
    temperature_c,
    shortwave_radiation_w_per_m2,
    -- W/m2 sustained for one hour = Wh/m2; x area / 1000 = kWh for the whole greenhouse
    lighting_w_per_m2 * {{ var("greenhouse_area_m2") }} / 1000 as lighting_kwh,
    {{ var("base_load_w_per_m2") }} * {{ var("greenhouse_area_m2") }} / 1000 as base_load_kwh,
    (lighting_w_per_m2 + {{ var("base_load_w_per_m2") }}) * {{ var("greenhouse_area_m2") }} / 1000 as electricity_kwh,
    heat_demand_w_per_m2 * {{ var("greenhouse_area_m2") }} / 1000 as heat_demand_kwh,
    heat_demand_w_per_m2 * {{ var("greenhouse_area_m2") }} / 1000
        / ({{ var("boiler_efficiency") }} * {{ var("gas_kwh_per_m3") }}) as gas_m3

from demand
