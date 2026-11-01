-- Fails when the air quality fact repeats its station, pollutant and hour
-- grain.
select location_id, pollutant, measured_at_utc
from {{ ref('fct_air_quality_hourly') }}
group by location_id, pollutant, measured_at_utc
having count(*) > 1
