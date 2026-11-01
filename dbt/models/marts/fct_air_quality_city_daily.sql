-- Aggregate fact: one row per city, pollutant and UTC date. Only station days
-- that pass the coverage rule enter the average, with equal weight per
-- station: the city value is the mean of the station daily means, never a
-- mean of hours or a rolling average. A city day is valid when enough
-- stations contributed.
with station_days as (
    select
        daily.location_id,
        daily.pollutant,
        daily.date_utc,
        daily.value_mean,
        stations.city_id
    from {{ ref('int_air_quality_daily') }} as daily
    inner join {{ ref('dim_station') }} as stations
        on stations.location_id = daily.location_id
    where daily.is_valid_day
      and stations.city_id is not null
),
daily as (
    select
        city_id,
        pollutant,
        date_utc,
        cast(strftime(date_utc, '%Y%m%d') as integer) as date_key,
        value_mean
    from station_days
)
select
    city_id,
    pollutant,
    date_key,
    avg(value_mean) as value_mean,
    count(*) as n_stations,
    count(*) >= {{ var('min_city_stations') }} as is_valid_city_day
from daily
group by city_id, pollutant, date_key
