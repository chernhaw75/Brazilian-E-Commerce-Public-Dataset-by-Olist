{{ config(materialized='view') }}

with order_items as (
    select
        order_id,
        cast(order_item_id as int) as order_item_id,
        product_id,
        seller_id,
        cast(NULLIF(shipping_limit_date,'') as timestamp) as shipping_limit_date,
        cast(price as float64) as price,
        cast(freight_value as float64) as freight_value
    from {{ source('raw_data', 'olist_order_items') }}
)

select * from order_items