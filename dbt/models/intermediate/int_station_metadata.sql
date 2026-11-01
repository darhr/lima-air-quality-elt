-- One row per location observed in the raw OpenAQ measurements, with the
-- name rescued from the measurements themselves and the first and last
-- measured hour. The station catalog is only used to enrich this set, so a
-- location that disappears from OpenAQ stays here while its raw data exists.
select
    location_id,
    max(location_name) as location_name,
    min(measured_at_utc) as first_measured_at_utc,
    max(measured_at_utc) as last_measured_at_utc
from {{ ref('stg_openaq__measurements') }}
group by location_id
