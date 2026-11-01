-- Fails when the aggregate city-day weather fact repeats its grain.
select city_id, date_key
from {{ ref('fct_weather_city_daily') }}
group by city_id, date_key
having count(*) > 1
