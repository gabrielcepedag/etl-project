SELECT
    *
FROM {{ source("earthquakes", "raw_earthquakes")}}