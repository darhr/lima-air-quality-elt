-- Aggregate fact: one row per city and UTC date, aggregated only from
-- physically valid hours.
-- Radiation is converted from hourly W/m2 to the daily MJ/m2 total; wind
-- direction uses a circular mean so 350 and 10 degrees average to 0, not 180.
-- The day is valid when hourly coverage reaches the weather_day threshold.
with thresholds as (
    select min_coverage_pct
    from {{ ref('coverage_thresholds') }}
    where scope = 'weather_day'
),
daily as (
    select
        city_id,
        cast(measured_at_utc as date) as date_utc,
        sum(allsky_sfc_sw_dwn) * 3600 / 1000000 as radiation_sum_mj,
        sum(clrsky_sfc_sw_dwn) * 3600 / 1000000 as clrsky_radiation_sum_mj,
        avg(t2m) as temp_mean,
        min(t2m) as temp_min,
        max(t2m) as temp_max,
        avg(t2mdew) as dew_mean,
        avg(rh2m) as rh_mean,
        avg(ws10m) as wind_speed_mean,
        max(ws10m) as wind_speed_max,
        (
            degrees(atan2(
                avg(sin(radians(wd10m))),
                avg(cos(radians(wd10m)))
            )) + 360
        ) % 360 as wind_dir_dominant,
        sum(prectotcorr) as precip_sum,
        avg(ps) as pressure_mean,
        count(*) as n_hours
    from {{ ref('int_weather_hourly') }}
    where is_valid
    group by city_id, cast(measured_at_utc as date)
)
select
    city_id,
    cast(strftime(date_utc, '%Y%m%d') as integer) as date_key,
    radiation_sum_mj,
    clrsky_radiation_sum_mj,
    temp_mean,
    temp_min,
    temp_max,
    dew_mean,
    rh_mean,
    wind_speed_mean,
    wind_speed_max,
    wind_dir_dominant,
    precip_sum,
    pressure_mean,
    n_hours,
    n_hours / 24.0 * 100 as coverage_pct,
    n_hours / 24.0 * 100 >= thresholds.min_coverage_pct as is_valid_day
from daily
cross join thresholds
