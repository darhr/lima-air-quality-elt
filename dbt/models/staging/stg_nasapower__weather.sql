-- One row per city-hour with one column per NASA POWER variable. The raw
-- payload holds one JSON object per variable ({'YYYYMMDDHH': value}), so the
-- two json_each calls unroll variables first and hours second, and the pivot
-- at the end restores the hourly grain. Fill values (-999.0) become NULL
-- here; the raw layer keeps them as the source sent them.
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
    strptime(hour_key, '%Y%m%d%H') as ts_utc,
    max(case when variable = 'ALLSKY_SFC_SW_DWN' then value end) as allsky_sfc_sw_dwn,
    max(case when variable = 'CLRSKY_SFC_SW_DWN' then value end) as clrsky_sfc_sw_dwn,
    max(case when variable = 'T2M' then value end) as t2m,
    max(case when variable = 'T2MDEW' then value end) as t2mdew,
    max(case when variable = 'RH2M' then value end) as rh2m,
    max(case when variable = 'WS10M' then value end) as ws10m,
    max(case when variable = 'WD10M' then value end) as wd10m,
    max(case when variable = 'PRECTOTCORR' then value end) as prectotcorr,
    max(case when variable = 'PS' then value end) as ps
from hours
group by city_id, ts_utc
