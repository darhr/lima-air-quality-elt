-- One row per station observed in the measurements, enriched with the current
-- OpenAQ catalog when the location is still listed. The station set comes from
-- data, so a location that disappears from the catalog keeps its row while its
-- measurements exist. When the project serves a single city, a documented
-- fallback assigns that city (and its timezone) to stations whose metadata is
-- no longer available, so their historical days can still reach the city mart.
with stations as (
    select * from {{ ref('int_station_metadata') }}
),
catalog as (
    select * from {{ ref('stg_openaq__locations') }}
),
cities as (
    select * from {{ ref('dim_city') }}
),
fallback as (
    select
        min(city_id) as city_id,
        min(timezone) as timezone
    from cities
    having count(*) = 1
),
assigned as (
    select
        stations.location_id,
        coalesce(
            catalog.name,
            stations.location_name,
            'location ' || cast(stations.location_id as varchar)
        ) as name,
        catalog.locality,
        catalog.country_code,
        coalesce(catalog.timezone, fallback.timezone) as timezone,
        catalog.latitude,
        catalog.longitude,
        catalog.provider_id,
        catalog.provider_name,
        catalog.is_mobile,
        catalog.is_monitor,
        catalog.licenses,
        catalog.location_id is not null as is_in_catalog,
        stations.first_measured_at_utc,
        stations.last_measured_at_utc,
        coalesce(city.city_id, fallback.city_id) as city_id
    from stations
    left join catalog
        on catalog.location_id = stations.location_id
    left join cities as city
        on catalog.latitude between city.bbox_min_lat and city.bbox_max_lat
       and catalog.longitude between city.bbox_min_lon and city.bbox_max_lon
    left join fallback on true
)
select * from assigned
