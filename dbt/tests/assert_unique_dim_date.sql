-- Fails when the date dimension repeats a date_key.
select date_key
from {{ ref('dim_date') }}
group by date_key
having count(*) > 1
