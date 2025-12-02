{{ config(materialized='view') }}

with order_payments as (
    select
        order_id,
        cast(payment_sequential as int) as payment_sequential,
        CASE
            WHEN payment_type ="not_defined" THEN "unknown"
            ELSE payment_type
        END AS payment_type,
        cast(payment_installments as int) as payment_installments,
        cast(payment_value as float64) as payment_value
    from {{ source('raw_data', 'olist_order_payments') }}
)

select * from order_payments