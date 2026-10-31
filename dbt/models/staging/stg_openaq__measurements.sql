-- One row per sensor-hour, flattened from the raw OpenAQ payload. The
-- timestamp is the start of the hour in UTC (the 'Z' suffix is stripped
-- before casting to a naive TIMESTAMP). o3 arrives in ug/m3 and ppm
-- depending on the sensor: values in ppm are converted to the canonical
-- ug/m3 (1 ppm = 1963 ug/m3 at 25 C, 1 atm). If a future sensor reports
-- another parameter-unit pair, this conversion needs a new case branch.
with measurements as (
    select
        location_id,
        sensor_id,
        payload::json as payload
    from {{ source('openaq_raw', 'measurements') }}
)
select
    location_id,
    sensor_id,
    json_extract_string(payload, '$.parameter.name') as parameter,
    case
        when json_extract_string(payload, '$.parameter.name') = 'o3'
             and json_extract_string(payload, '$.parameter.units') = 'ppm'
            then json_extract(payload, '$.value')::double * 1963.0
        else json_extract(payload, '$.value')::double
    end as value,
    'ug/m3' as units,
    json_extract(payload, '$.coverage.percentComplete')::double as coverage_pct,
    cast(
        replace(json_extract_string(payload, '$.period.datetimeFrom.utc'), 'Z', '')
        as timestamp
    ) as ts_utc
from measurements
