-- One row per city and UTC date in the configured spine. Keys and calendar
-- attributes are UTC; local time attributes belong to the station facts.
with dates as (
    select cast(day as date) as date_utc
    from generate_series(
        timestamp '{{ var("date_spine_start") }}',
        timestamp '{{ var("date_spine_end") }}',
        interval 1 day
    ) as spine(day)
)
select
    cast(strftime(date_utc, '%Y%m%d') as integer) as date_key,
    date_utc,
    year(date_utc) as year,
    month(date_utc) as month,
    day(date_utc) as day,
    dayofweek(date_utc) as day_of_week,
    strftime(date_utc, '%A') as day_name,
    strftime(date_utc, '%B') as month_name,
    dayofweek(date_utc) in (0, 6) as is_weekend
from dates
