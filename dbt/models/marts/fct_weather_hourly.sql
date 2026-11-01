-- One row per city and hour UTC, with the dimension keys added. The weather
-- grain is already city-level because the exposure is a single NASA POWER
-- point per city.
select
    city_id,
    cast(strftime(measured_at_utc, '%Y%m%d') as integer) as date_key,
    extract(hour from measured_at_utc)::integer as hour_key,
    measured_at_utc,
    allsky_sfc_sw_dwn,
    clrsky_sfc_sw_dwn,
    t2m,
    t2mdew,
    rh2m,
    ws10m,
    wd10m,
    prectotcorr,
    ps,
    is_valid
from {{ ref('int_weather_hourly') }}
