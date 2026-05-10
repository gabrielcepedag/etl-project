with source as (
    select * from {{ source('earthquakes', 'raw_earthquakes') }}
),
cleaned as (
    select
        id,
        mag,
        place,
        -- convertir epoch a timestamp
        to_timestamp(time / 1000) as event_time,
        to_timestamp(updated / 1000) as updated_time,
        tsunami,
        sig,
        net,
        code,
        magtype,
        type,
        status,
        -- PARSE coordinates
        parse_json(coordinates)[0]::float as longitude,
        parse_json(coordinates)[1]::float as latitude,
        parse_json(coordinates)[2]::float as depth,
        inserted_at
    from source
)
select * from cleaned
