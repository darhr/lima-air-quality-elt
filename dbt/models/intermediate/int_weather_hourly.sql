-- One row per city-hour, pivoting the long staging model back to one column
-- per NASA POWER variable. The physical validity flag requires every variable
-- to be present and inside its range, so a fill turned into NULL invalidates
-- the hour. No dedup is needed: the ingestion guarantees one row per city and
-- day and each hour lives in a single partition.
with pivoted as (
    select
        city_id,
        measured_at_utc,
        max(case when variable = 'ALLSKY_SFC_SW_DWN' then value end) as allsky_sfc_sw_dwn,
        max(case when variable = 'CLRSKY_SFC_SW_DWN' then value end) as clrsky_sfc_sw_dwn,
        max(case when variable = 'T2M' then value end) as t2m,
        max(case when variable = 'T2MDEW' then value end) as t2mdew,
        max(case when variable = 'RH2M' then value end) as rh2m,
        max(case when variable = 'WS10M' then value end) as ws10m,
        max(case when variable = 'WD10M' then value end) as wd10m,
        max(case when variable = 'PRECTOTCORR' then value end) as prectotcorr,
        max(case when variable = 'PS' then value end) as ps
    from {{ ref('stg_nasapower__weather') }}
    group by city_id, measured_at_utc
)
select
    city_id,
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
    coalesce(
        allsky_sfc_sw_dwn between 0 and 1500
        and clrsky_sfc_sw_dwn between 0 and 1500
        and t2m between -10 and 45
        and t2mdew between -10 and 45
        and rh2m between 0 and 100
        and ws10m between 0 and 40
        and wd10m between 0 and 360
        and prectotcorr between 0 and 100
        and ps between 80 and 110,
        false
    ) as is_valid
from pivoted
