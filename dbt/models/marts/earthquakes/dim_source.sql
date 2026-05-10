select distinct
    {{ dbt_utils.generate_surrogate_key(['net','code']) }} as source_id,
    net,
    code
from {{ ref('stg_earthquakes') }}