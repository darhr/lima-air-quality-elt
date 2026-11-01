-- Fails when the raw measurements repeat a sensor-hour. Ingestion rejects this
-- case per partition; the test protects the staging contract if the raw is
-- rebuilt or edited outside the pipeline.
select sensor_id, measured_at_utc
from {{ ref('stg_openaq__measurements') }}
group by sensor_id, measured_at_utc
having count(*) > 1
