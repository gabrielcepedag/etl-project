-- Earthquake events cannot occur in the future.
-- Future timestamps indicate a timestamp parsing bug or bad source data.
select id, event_time
from {{ ref('fact_earthquakes') }}
where event_time > current_timestamp()
