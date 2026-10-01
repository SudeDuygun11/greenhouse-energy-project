-- One row per gas day. A gas day D runs from 06:00 local on D to 06:00 local on D+1.
-- Source rows are stamped 04:00/05:00 UTC (= 06:00 local). Chunk boundaries in the
-- backfill added a second, 00:00 UTC row for some days; we keep the latest row per day.

with source as (

    select * from {{ source('energyzero', 'gas_prices') }}

),

deduplicated as (

    select
        date(timestamp_utc, '{{ var("local_timezone") }}') as gas_day,
        price_eur_per_m3,
        _dlt_load_id
    from source
    qualify row_number() over (
        partition by date(timestamp_utc, '{{ var("local_timezone") }}')
        order by timestamp_utc desc
    ) = 1

)

select
    gas_day,
    timestamp(datetime(gas_day, time(6, 0, 0)), '{{ var("local_timezone") }}') as valid_from_utc,
    timestamp(datetime(date_add(gas_day, interval 1 day), time(6, 0, 0)), '{{ var("local_timezone") }}') as valid_to_utc,
    price_eur_per_m3 as gas_price_eur_per_m3,
    _dlt_load_id

from deduplicated
