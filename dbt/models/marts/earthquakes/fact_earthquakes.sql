select
    e.id,
    e.mag,
    e.sig,
    e.tsunami,

    l.location_id,
    s.source_id,

    e.event_time

from {{ ref('stg_earthquakes') }} e
left join {{ ref('dim_location') }} l
    on e.latitude = l.latitude
    and e.longitude = l.longitude
    and e.depth = l.depth
left join {{ ref('dim_source') }} s
    on e.net = s.net
    and e.code = s.code