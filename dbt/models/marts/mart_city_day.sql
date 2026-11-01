{{ config(materialized='view') }}

-- Consumable city-day view: air quality left joined with weather by city and
-- date, plus city and date attributes. Air quality drives the row, so no valid
-- AQ day is lost when the weather day is missing or invalid.
select
    air.city_id,
    air.pollutant,
    air.date_key,
    dates.date_utc,
    cities.name as city_name,
    air.value_mean,
    air.n_stations,
    air.is_valid_city_day as aq_valid_city_day,
    weather.city_id is not null as has_weather,
    weather.radiation_sum_mj,
    weather.clrsky_radiation_sum_mj,
    weather.temp_mean,
    weather.temp_min,
    weather.temp_max,
    weather.dew_mean,
    weather.rh_mean,
    weather.wind_speed_mean,
    weather.wind_speed_max,
    weather.wind_dir_dominant,
    weather.precip_sum,
    weather.pressure_mean,
    weather.n_hours as weather_n_hours,
    weather.coverage_pct as weather_coverage_pct,
    weather.is_valid_day as weather_valid_day
from {{ ref('fct_air_quality_city_daily') }} as air
left join {{ ref('fct_weather_city_daily') }} as weather
    on weather.city_id = air.city_id
   and weather.date_key = air.date_key
left join {{ ref('dim_city') }} as cities
    on cities.city_id = air.city_id
left join {{ ref('dim_date') }} as dates
    on dates.date_key = air.date_key
