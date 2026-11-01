-- One row per station, pollutant and UTC day. The city layer averages these
-- station days with equal weight, so the coverage rule lives here: a day is
-- valid when the share of valid hours over the 24 expected hours reaches the
-- station_day threshold of the seed. Reported and valid hours are kept
-- separately so missing and out-of-range data can be told apart.
with thresholds as (
    select min_coverage_pct
    from {{ ref('coverage_thresholds') }}
    where scope = 'station_day'
),
daily as (
    select
        location_id,
        pollutant,
        cast(measured_at_utc as date) as date_utc,
        avg(value) filter (where is_valid) as value_mean,
        min(value) filter (where is_valid) as value_min,
        max(value) filter (where is_valid) as value_max,
        count(*) as n_hours_reported,
        count(*) filter (where is_valid) as n_hours_valid
    from {{ ref('int_air_quality_hourly') }}
    group by location_id, pollutant, cast(measured_at_utc as date)
)
select
    daily.location_id,
    daily.pollutant,
    daily.date_utc,
    daily.value_mean,
    daily.value_min,
    daily.value_max,
    daily.n_hours_reported,
    daily.n_hours_valid,
    daily.n_hours_valid / 24.0 * 100 as coverage_pct,
    daily.n_hours_valid / 24.0 * 100 >= thresholds.min_coverage_pct as is_valid_day
from daily
cross join thresholds
