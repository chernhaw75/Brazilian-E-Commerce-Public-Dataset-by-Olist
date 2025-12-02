{{ config(materialized='table') }}

WITH max_date AS(
    select
        max(order_purchase_timestamp) as max_order_date
    from {{ ref('fact_orders') }}
),

customer_orders AS(
    select
        f.customer_unique_id,
        count(f.order_id) as total_orders,
        sum(f.total_order_value) as total_revenue,
        avg(f.total_order_value) as avg_order_value,
        avg(f.num_items) as avg_items_per_order,
        avg(f.total_freight) as avg_freight_per_order,
        sum(f.late_delivery_flag)/count(f.order_id) as late_delivery_rate,
        sum(f.multiple_sellers)/count(f.order_id) as pct_multiple_sellers,

        -- Payment behavior
        sum(f.payment_type_credit_card)/count(f.order_id) as pct_credit_card,
        sum(f.payment_type_debit_card)/count(f.order_id) as pct_debit_card,
        sum(f.payment_type_voucher)/count(f.order_id) as pct_voucher,
        sum(f.payment_type_boleto)/count(f.order_id) as pct_boleto,
        avg(f.total_payment) as avg_payment_value,
        avg(f.payment_type_credit_card*f.total_payment) as avg_credit_card_payment,
        case when sum(f.payment_type_credit_card) > sum(f.payment_type_debit_card) then 1 else 0 end as pref_credit_over_debit,
        case when sum(f.payment_type_credit_card) > sum(f.payment_type_boleto) then 1 else 0 end as pref_credit_over_boleto

    from {{ ref('fact_orders') }} f
    group by f.customer_unique_id
),

customer_info as (
    select
        c.customer_id,
        c.customer_unique_id,
        c.customer_zip,
        c.customer_city,
        c.customer_state,
        c.first_purchase_timestamp,
        c.last_purchase_timestamp,
        c.total_orders as dim_total_orders,
        c.avg_days_between_orders,
        c.stddev_days_between_orders,
        c.total_revenue as dim_total_revenue,
        c.avg_order_value as dim_avg_order_value,
        c.days_since_last_purchase
    
    from {{ ref('dim_customers') }} c
),

rfm_buckets as(
    select
        customer_unique_id,
        NTILE(5) over (order by days_since_last_purchase ASC) as r_bucket,
        NTILE(5) over (order by dim_total_orders desc) as f_bucket,
        NTILE(5) over (order by dim_total_revenue desc) as m_bucket

    from customer_info
)

select
    ci.customer_id,
    ci.customer_unique_id,
    ci.customer_zip,
    ci.customer_city,
    ci.customer_state,
    ci.first_purchase_timestamp,
    ci.last_purchase_timestamp,
    ci.dim_total_orders,
    ci.avg_days_between_orders,
    ci.stddev_days_between_orders,
    ci.dim_total_revenue,
    ci.dim_avg_order_value,
    ci.days_since_last_purchase,

    -- Aggregated order metrics
    co.total_orders,
    co.total_revenue,
    co.avg_order_value,
    co.avg_items_per_order,
    co.avg_freight_per_order,
    co.late_delivery_rate,
    co.pct_multiple_sellers,

    --Payment behavior
    co.pct_credit_card,
    co.pct_debit_card,
    co.pct_voucher,
    co.pct_boleto,
    co.avg_payment_value,
    co.avg_credit_card_payment,
    co.pref_credit_over_debit,
    co.pref_credit_over_boleto,

    --RFM buckets
    rb.r_bucket,
    rb.f_bucket,
    rb.m_bucket

from customer_orders co
left join customer_info ci on co.customer_unique_id = ci.customer_unique_id
left join rfm_buckets rb on co.customer_unique_id = rb.customer_unique_id