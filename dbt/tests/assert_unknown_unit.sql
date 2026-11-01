-- Fails when OpenAQ sends a pollutant unit the model cannot normalize. The
-- affected rows are kept with a NULL canonical value for audit, but they must
-- not reach the analytical layer silently.
select
    location_id,
    sensor_id,
    pollutant,
    source_unit
from {{ ref('stg_openaq__measurements') }}
where canonical_unit is null
