-- Fails when the station dimension has more than one row per location_id.
select location_id
from {{ ref('dim_station') }}
group by location_id
having count(*) > 1
