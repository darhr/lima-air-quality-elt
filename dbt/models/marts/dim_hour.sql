-- One row per hour of the day (0-23), conformed between both facts.
select
    cast(hour_of_day as integer) as hour_key
from range(0, 24) as hours(hour_of_day)
