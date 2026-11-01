-- One row per location, pollutant and hour: the sensor with the highest
-- hourly coverage wins and ties break by sensor id, so the choice never
-- depends on row order. A sensor with an unconvertible unit (NULL value) is
-- ranked last. Values are checked against the canonical range of the
-- pollutant seed. Local time attributes belong to the fact table, which is
-- the layer that can join dim_station.
with ranked as (
    select
        measurements.*,
        row_number() over (
            partition by location_id, pollutant, measured_at_utc
            order by value is null, coverage_pct desc nulls last, sensor_id
        ) as sensor_rank
    from {{ ref('stg_openaq__measurements') }} as measurements
)
select
    ranked.location_id,
    ranked.sensor_id,
    ranked.pollutant,
    ranked.value,
    ranked.canonical_unit,
    ranked.coverage_pct,
    ranked.measured_at_utc,
    coalesce(
        ranked.value between pollutants.min_valid and pollutants.max_valid,
        false
    ) as is_valid
from ranked
inner join {{ ref('pollutant_units') }} as pollutants
    on pollutants.pollutant = ranked.pollutant
where ranked.sensor_rank = 1
