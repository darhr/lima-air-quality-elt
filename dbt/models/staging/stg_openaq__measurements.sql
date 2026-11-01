-- One row per sensor-hour, flattened from the raw OpenAQ payload. The
-- timestamp is the start of the hour in UTC (the 'Z' suffix is stripped
-- before casting to a naive TIMESTAMP). o3 arrives in ug/m3 and ppm
-- depending on the sensor: values in ppm are converted to the canonical
-- ug/m3 (1 ppm = 1963 ug/m3 at 25 C, 1 atm). source_unit is kept for audit;
-- rows with an unknown unit keep a NULL canonical value so the dedicated
-- test fails instead of silently mislabeling the measurement.
with measurements as (
    select
        location_id,
        location_name,
        sensor_id,
        payload::json as payload
    from {{ source('openaq_raw', 'measurements') }}
),
parsed as (
    select
        location_id,
        location_name,
        sensor_id,
        json_extract_string(payload, '$.parameter.name') as pollutant,
        json_extract_string(payload, '$.parameter.units') as source_unit,
        json_extract(payload, '$.value')::double as raw_value,
        json_extract(payload, '$.coverage.percentComplete')::double as coverage_pct,
        cast(
            replace(json_extract_string(payload, '$.period.datetimeFrom.utc'), 'Z', '')
            as timestamp
        ) as measured_at_utc
    from measurements
),
normalized as (
    select
        *,
        case
            when source_unit in ('ug/m3', 'µg/m³') then raw_value
            when pollutant = 'o3' and source_unit = 'ppm' then raw_value * 1963.0
        end as value
    from parsed
    where pollutant in ('pm25', 'pm10', 'o3')
)
select
    location_id,
    location_name,
    sensor_id,
    pollutant,
    value,
    case
        when source_unit in ('ug/m3', 'µg/m³') then 'ug/m3'
        when pollutant = 'o3' and source_unit = 'ppm' then 'ug/m3'
    end as canonical_unit,
    source_unit,
    coverage_pct,
    measured_at_utc
from normalized
