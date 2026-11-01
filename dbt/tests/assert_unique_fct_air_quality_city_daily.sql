-- Fails when the aggregate city-day air quality fact repeats its grain.
select city_id, pollutant, date_key
from {{ ref('fct_air_quality_city_daily') }}
group by city_id, pollutant, date_key
having count(*) > 1
