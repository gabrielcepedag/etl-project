-- The tsunami column is a binary flag: 1 = tsunami message issued, 0 = not.
-- Any other value is invalid.
select id, tsunami_caused
from {{ ref('fact_earthquakes') }}
where tsunami_caused not in (0, 1)
