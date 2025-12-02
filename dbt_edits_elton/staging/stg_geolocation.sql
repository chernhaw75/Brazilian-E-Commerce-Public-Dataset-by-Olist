{{ config(materialized='view') }}

with geolocation as (
    select
        cast(geolocation_zip_code_prefix as int) as geo_zip,
        cast(geolocation_lat as float64) as geolocation_lat,
        cast(geolocation_lng as float64) as geolocation_lng,
        geolocation_city,
        geolocation_state
    from {{ source('raw_data', 'olist_geolocation') }}
)

select * from geolocation