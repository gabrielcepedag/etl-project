-- Latitude must be -90 to 90, longitude -180 to 180, depth >= 0 (km below surface).
-- Violations indicate a coordinate parsing error from the GeoJSON source.
select
    location_id,
    latitude,
    longitude,
    depth
from {{ ref('dim_location') }}
where
    latitude  < -90  or latitude  > 90
    or longitude < -180 or longitude > 180
    or depth < 0
