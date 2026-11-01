-- Fails when the hour dimension repeats an hour_key.
select hour_key
from {{ ref('dim_hour') }}
group by hour_key
having count(*) > 1
