{{
    config(
        materialized='table'
    )
}}

with daily_agg as (
    select
        cast(date_trunc('day', f.event_time) as date) as event_date,
        l.latitude,
        l.longitude,
        l.depth,
        count(f.id)                         as event_count,
        avg(f.mag)                          as avg_magnitude,
        max(f.mag)                          as max_magnitude,
        min(f.mag)                          as min_magnitude,
        sum(f.sig)                          as total_significance,
        sum(f.tsunami)                      as tsunami_events
    from {{ ref('fact_earthquakes') }} f
    left join {{ ref('dim_location') }} l
        on f.location_id = l.location_id
    where
        f.mag is not null
        and f.event_time is not null
    group by 1, 2, 3, 4
),

ranked as (
    select
        *,
        -- Window function to rank different dates depending on a 
        -- score that's calculated by using average magnitude and total number of events.
        rank() over (
            partition by event_date
            order by event_count desc
        )                                   as daily_location_rank,
        round(avg_magnitude * event_count, 2) as activity_score
    from daily_agg
)

select
    event_date,
    latitude,
    longitude,
    depth,
    event_count,
    round(avg_magnitude, 3)                 as avg_magnitude,
    max_magnitude,
    min_magnitude,
    total_significance,
    tsunami_events,
    daily_location_rank,
    activity_score
from ranked
order by
    event_date desc,
    activity_score desc
