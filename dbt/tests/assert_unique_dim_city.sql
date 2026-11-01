-- Fails when the city dimension has more than one row per city_id.
select city_id
from {{ ref('dim_city') }}
group by city_id
having count(*) > 1
