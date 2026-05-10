select distinct
    {{ dbt_utils.generate_surrogate_key(['latitude','longitude','depth']) }} as location_id,
    latitude,
    longitude,
    depth
from {{ ref('stg_earthquakes') }}