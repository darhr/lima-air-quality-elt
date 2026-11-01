-- Fails when the weather fact repeats its city and hour grain.
select city_id, measured_at_utc
from {{ ref('fct_weather_hourly') }}
group by city_id, measured_at_utc
having count(*) > 1
