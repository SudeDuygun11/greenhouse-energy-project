-- Expands the monthly profile seed into one row per UTC hour. Lighting and day/night
-- windows are evaluated in local time, so DST shifts are handled by the conversion.

with hours as (

    select * from {{ ref('int_calendar__hours') }}

),

profile as (

    select * from {{ ref('greenhouse_monthly_profile') }}

)

select
    hours.hour_utc,
    hours.hour_local,
    hours.date_local,
    hours.hour_of_day_local,
    hours.hour_of_day_local >= {{ var("day_start_hour") }}
        and hours.hour_of_day_local < {{ var("day_end_hour") }} as is_day,
    coalesce(
        profile.lighting_enabled
            and hours.hour_of_day_local >= profile.lights_on_hour
            and hours.hour_of_day_local < profile.lights_off_hour,
        false
    ) as is_lighting_scheduled,
    case
        when hours.hour_of_day_local >= {{ var("day_start_hour") }}
            and hours.hour_of_day_local < {{ var("day_end_hour") }}
            then profile.heating_day_setpoint_c
        else profile.heating_night_setpoint_c
    end as heating_setpoint_c

from hours
inner join profile
    on profile.month = extract(month from hours.hour_local)
