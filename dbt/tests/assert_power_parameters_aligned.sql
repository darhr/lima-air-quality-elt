-- Fails when an ingested NASA POWER hour does not carry all nine variables.
-- The raw layer is validated on ingestion, so this protects the staging
-- unpivot if the contract changes.
with expected_variables as (
    select 9 as n_variables
),
hours as (
    select
        city_id,
        measured_at_utc,
        count(distinct variable) as n_variables
    from {{ ref('stg_nasapower__weather') }}
    group by city_id, measured_at_utc
)
select hours.*
from hours
cross join expected_variables
where hours.n_variables <> expected_variables.n_variables
