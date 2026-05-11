select
    e.id as ID,
    e.mag as MAGNITUDE,
    e.sig as SIGNIFICANCE,
    e.tsunami AS TSUNAMI_CAUSED,
    -- Derived risk index: combines magnitude and significance into a single score
    round(cast(e.mag as float) * cast(e.sig as float) / 1000.0, 4) as RISK_INDEX,
    l.location_id as LOCATION_ID,
    s.source_id as SOURCE_ID, 

    e.event_time

from {{ ref('stg_earthquakes') }} e
left join {{ ref('dim_location') }} l
    on e.latitude = l.latitude
    and e.longitude = l.longitude
    and e.depth = l.depth
left join {{ ref('dim_source') }} s
    on e.net = s.net
    and e.code = s.code
where
    e.mag is not null
    and e.id is not null