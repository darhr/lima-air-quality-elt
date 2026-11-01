-- The city validity flag must match the station threshold variable, so the
-- mart never disagrees with the documented rule.
select
    city_id,
    pollutant,
    date_key,
    n_stations,
    is_valid_city_day
from {{ ref('fct_air_quality_city_daily') }}
where is_valid_city_day <> (n_stations >= {{ var('min_city_stations') }})
