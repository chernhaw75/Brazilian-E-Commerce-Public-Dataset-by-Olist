{{ config(materialized='view') }}

with sellers as (
    select
        seller_id,
        seller_zip_code_prefix AS seller_zip,
        seller_city,
        seller_state
    from {{ source('raw_data', 'olist_sellers') }}
)

select * from sellers