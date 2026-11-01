-- Coverage is always a percentage of the 24 expected hours, so it must stay
-- inside 0-100. Fails when an aggregation divides by the wrong denominator.
select 'air_quality' as model, location_id as key, coverage_pct
from {{ ref('int_air_quality_daily') }}
where coverage_pct < 0 or coverage_pct > 100
union all
select 'weather' as model, city_id as key, coverage_pct
from {{ ref('fct_weather_city_daily') }}
where coverage_pct < 0 or coverage_pct > 100
