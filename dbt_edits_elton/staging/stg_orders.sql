{{ config(materialized='view') }}

with orders as (
    select
        order_id,
        customer_id,
        order_status,
        cast(NULLIF(order_purchase_timestamp,'') as TIMESTAMP) as order_purchase_timestamp,
        cast(NULLIF(order_approved_at,'') as TIMESTAMP) as order_approved_at,
        cast(NULLIF(order_delivered_carrier_date,'') as TIMESTAMP) as order_delivered_carrier_date,
        cast(NULLIF(order_delivered_customer_date,'') as TIMESTAMP) as order_delivered_customer_date,
        cast(NULLIF(order_estimated_delivery_date,'') as TIMESTAMP) as order_estimated_delivery_date
    from {{ source('raw_data', 'olist_orders') }}
)

select * from orders