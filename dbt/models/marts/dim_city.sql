-- One row per city served by the pipeline; the bounding box is used to assign
-- stations to a city and the point is the single NASA POWER exposure point.
select
    city_id,
    name,
    country_code,
    timezone,
    point_lat,
    point_lon,
    bbox_min_lat,
    bbox_max_lat,
    bbox_min_lon,
    bbox_max_lon
from {{ ref('cities') }}
