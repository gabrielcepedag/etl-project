-- USGS defines significance as an integer between 0 and 1000.
-- Values outside this range indicate a parsing or ingestion error.
select id, significance
from {{ ref('fact_earthquakes') }}
where significance < 0 or significance > 1000
