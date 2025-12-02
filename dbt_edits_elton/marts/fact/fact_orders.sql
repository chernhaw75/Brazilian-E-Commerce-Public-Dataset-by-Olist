{{ config(materialized='table') }}

WITH base AS (
    select
        o.order_id,
        o.customer_id,
        o.order_purchase_timestamp,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,

        --Order-level flags and measures
        o.num_items,
        o.total_order_value,
        o.total_freight,
        o.seller_count,
        o.multiple_sellers,
        o.primary_seller_id,
        o.late_delivery_flag,

        --Payment info
        p.total_payment,
        p.payment_type_credit_card,
        p.payment_type_debit_card,
        p.payment_type_voucher,
        p.payment_type_boleto

    from {{ ref('dim_orders') }} o
    left join {{ ref('dim_order_payments') }} p on o.order_id = p.order_id
),

customer_info AS(
    select
        c.customer_id,
        c.customer_unique_id,
        c.customer_zip,
        c.customer_city,
        c.customer_state

    from {{ ref('dim_customers') }} c
),

seller_info AS(
    select
        si.seller_id,
        si.late_delivery_rate,
        si.seller_zip,
        si.seller_city,
        si.seller_state

    from {{ ref('dim_sellers') }} si
)

select
    b.*,
    ci.customer_unique_id,
    ci.customer_zip,
    ci.customer_city,
    ci.customer_state,
    si.seller_zip,
    si.seller_city,
    si.seller_state,
    si.late_delivery_rate

from base b
left join customer_info ci on b.customer_id = ci.customer_id
left join seller_info si on b.primary_seller_id = si.seller_id