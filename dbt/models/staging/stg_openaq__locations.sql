-- One row per location in the OpenAQ bbox snapshot (current state of the
-- source, replaced on every ingest run). Licenses stays as the JSON string
-- the source sends; parsing it is a dim_station concern, not a staging one.
select
    location_id,
    name,
    locality,
    country_code,
    timezone,
    latitude,
    longitude,
    provider_id,
    provider_name,
    is_mobile,
    is_monitor,
    licenses
from {{ source('openaq_raw', 'locations') }}
