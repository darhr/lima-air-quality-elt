-- Fails when the locations snapshot carries the same location twice. A
-- duplicated dimension row would fan out the station fact and the city mean.
select location_id
from {{ ref('stg_openaq__locations') }}
group by location_id
having count(*) > 1
