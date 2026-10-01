with dates as (

    select distinct date_local
    from {{ ref('int_calendar__hours') }}

),

holidays as (

    select * from {{ ref('stg_nager_date__public_holidays') }}

)

select
    dates.date_local as date_day,
    extract(year from dates.date_local) as year,
    extract(quarter from dates.date_local) as quarter,
    extract(month from dates.date_local) as month,
    format_date('%B', dates.date_local) as month_name,
    extract(isoweek from dates.date_local) as iso_week,
    extract(dayofweek from dates.date_local) as day_of_week,  -- 1 = Sunday
    format_date('%A', dates.date_local) as day_name,
    extract(dayofweek from dates.date_local) in (1, 7) as is_weekend,
    case
        when extract(month from dates.date_local) in (12, 1, 2) then 'Winter'
        when extract(month from dates.date_local) in (3, 4, 5) then 'Spring'
        when extract(month from dates.date_local) in (6, 7, 8) then 'Summer'
        else 'Autumn'
    end as season,
    coalesce(holidays.is_public_holiday, false) as is_public_holiday,
    holidays.holiday_name,
    holidays.holiday_types,
    extract(dayofweek from dates.date_local) in (1, 7)
        or coalesce(holidays.is_public_holiday, false) as is_non_working_day

from dates
left join holidays
    on holidays.holiday_date = dates.date_local
