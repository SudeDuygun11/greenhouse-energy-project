-- One row per UTC hour from spine_start_date (local midnight) through the end of
-- tomorrow (local), so published day-ahead prices are covered.

with hours as (

    select hour_utc
    from unnest(generate_timestamp_array(
        timestamp('{{ var("spine_start_date") }}', '{{ var("local_timezone") }}'),
        timestamp_sub(
            timestamp(date_add(current_date('{{ var("local_timezone") }}'), interval 2 day), '{{ var("local_timezone") }}'),
            interval 1 hour
        ),
        interval 1 hour
    )) as hour_utc

)

select
    hour_utc,
    datetime(hour_utc, '{{ var("local_timezone") }}') as hour_local,
    date(hour_utc, '{{ var("local_timezone") }}') as date_local,
    extract(hour from datetime(hour_utc, '{{ var("local_timezone") }}')) as hour_of_day_local

from hours
