-- Daily greenhouse energy summary with baseline vs. optimized side by side, for the
-- dashboard. Grain: one row per local date. Joins to dim_date on date_day.

with hourly as (

    select * from {{ ref('fct_greenhouse_hourly_energy') }}

),

daily as (

    select
        date_local,
        {% for scenario in ['baseline', 'optimized'] %}
        countif(scenario = '{{ scenario }}' and is_lighting_on) as {{ scenario }}_lighting_hours,
        sum(if(scenario = '{{ scenario }}', electricity_kwh, 0)) as {{ scenario }}_electricity_kwh,
        sum(if(scenario = '{{ scenario }}', gas_m3, 0)) as {{ scenario }}_gas_m3,
        sum(if(scenario = '{{ scenario }}', electricity_cost_eur, null)) as {{ scenario }}_electricity_cost_eur,
        sum(if(scenario = '{{ scenario }}', gas_cost_eur, null)) as {{ scenario }}_gas_cost_eur,
        sum(if(scenario = '{{ scenario }}', total_energy_cost_eur, null)) as {{ scenario }}_total_cost_eur,
        sum(if(scenario = '{{ scenario }}', total_co2_kg, null)) as {{ scenario }}_co2_kg,
        {% endfor %}
        countif(scenario = 'baseline' and total_energy_cost_eur is null) as hours_missing_cost,
        count(distinct hour_utc) as hours_in_day
    from hourly
    group by date_local

)

select
    date_local as date_day,
    * except (date_local),
    baseline_total_cost_eur - optimized_total_cost_eur as cost_savings_eur,
    safe_divide(baseline_total_cost_eur - optimized_total_cost_eur, baseline_total_cost_eur) as cost_savings_pct,
    baseline_co2_kg - optimized_co2_kg as co2_savings_kg,
    hours_missing_cost = 0 as is_complete

from daily
