-- One row per city, variable and hour, unpivoted from the raw NASA POWER
-- payload. The raw holds one JSON object per variable ({'YYYYMMDDHH': value}),
-- so the two json_each calls unroll variables first and hours second. Fill
-- values (-999.0) become NULL here; the raw layer keeps them as sent. The
-- pivot back to one column per variable lives in int_weather_hourly.
with payload as (
    select
        city_id,
        payload::json as payload
    from {{ source('nasapower_raw', 'weather') }}
),
variables as (
    select
        payload.city_id,
        json_each.key as variable,
        json_each.value as hours
    from payload, json_each(payload -> '$.parameter')
),
hours as (
    select
        variables.city_id,
        variables.variable,
        json_each.key as hour_key,
        nullif(json_each.value::double, -999.0) as value
    from variables, json_each(variables.hours)
)
select
    city_id,
    variable,
    strptime(hour_key, '%Y%m%d%H') as measured_at_utc,
    value
from hours
