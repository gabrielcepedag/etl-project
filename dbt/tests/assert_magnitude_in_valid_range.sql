-- Richter scale runs from roughly -2 to 10.
-- Any event outside this range indicates a data quality issue upstream.
select id, magnitude
from {{ ref('fact_earthquakes') }}
where magnitude < -2 or magnitude > 10
