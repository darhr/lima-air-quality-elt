-- Fails when the pollutant dimension has more than one row per pollutant.
select pollutant
from {{ ref('dim_pollutant') }}
group by pollutant
having count(*) > 1
