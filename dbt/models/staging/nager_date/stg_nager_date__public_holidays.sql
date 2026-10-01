with holidays as (

    select * from {{ source('nager_date', 'public_holidays') }}

),

types as (

    select
        _dlt_parent_id,
        logical_or(value = 'Public') as is_public,
        string_agg(value, ', ' order by value) as holiday_types
    from {{ source('nager_date', 'public_holidays__types') }}
    group by _dlt_parent_id

)

select
    cast(holidays.date as date) as holiday_date,
    holidays.local_name as holiday_name_local,
    holidays.name as holiday_name,
    holidays.country_code,
    holidays.is_global,
    coalesce(types.is_public, false) as is_public_holiday,
    types.holiday_types,
    holidays._dlt_load_id

from holidays
left join types
    on types._dlt_parent_id = holidays._dlt_id
