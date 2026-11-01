-- One row per station, pollutant and hour UTC. Thin projection of the
-- intermediate model plus the dimension keys: city_id and the local time
-- attributes come from dim_station, which is the only layer that knows the
-- station timezone.
with localized as (
    select
        hourly.*,
        stations.city_id,
        timezone(stations.timezone, timezone('UTC', hourly.measured_at_utc)) as local_ts
    from {{ ref('int_air_quality_hourly') }} as hourly
    left join {{ ref('dim_station') }} as stations
        on stations.location_id = hourly.location_id
)
select
    location_id,
    pollutant,
    city_id,
    cast(strftime(measured_at_utc, '%Y%m%d') as integer) as date_key,
    extract(hour from measured_at_utc)::integer as hour_key,
    measured_at_utc,
    value,
    canonical_unit,
    coverage_pct,
    sensor_id,
    is_valid,
    cast(local_ts as date) as local_date,
    extract(hour from local_ts)::integer as local_hour
from localized
