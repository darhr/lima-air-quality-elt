-- Fails when a city-hour does not have exactly one row per NASA POWER
-- variable: a missing variable lowers the count and a duplicated one raises it,
-- so int_weather_hourly never has to resolve duplicates silently.
select city_id, measured_at_utc, count(*) as n_rows
from {{ ref('stg_nasapower__weather') }}
group by city_id, measured_at_utc
having count(*) <> 9
