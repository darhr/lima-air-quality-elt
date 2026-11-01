-- Fails when a station row or an air quality fact cannot be attributed to a
-- city. The single-city fallback in dim_station should keep this empty.
select 'dim_station' as model, location_id, cast(null as varchar) as pollutant
from {{ ref('dim_station') }}
where city_id is null
union all
select 'fct_air_quality_hourly' as model, location_id, pollutant
from {{ ref('fct_air_quality_hourly') }}
where city_id is null
