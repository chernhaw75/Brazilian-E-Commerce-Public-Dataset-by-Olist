{{ config(materialized='view') }}

with customers as (
    select
        customer_id,
        customer_unique_id,
        CAST(customer_zip_code_prefix as int) AS customer_zip,
        customer_city,
        customer_state
    from {{ source('raw_data', 'olist_customers') }}
)

select * from customers