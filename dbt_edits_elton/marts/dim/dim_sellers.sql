{{ config(materialized = 'table')}}

with seller_agg as (
    select
        s.seller_id,
        min(s.seller_zip) as seller_zip,
        min(s.seller_city) as seller_city,
        min(s.seller_state) as seller_state,
        coalesce(count(distinct oi.order_id), 0) as num_orders,
        coalesce(sum(oi.price + oi.freight_value), 0) as total_revenue
    from {{ ref('stg_sellers') }} s
    join {{ ref('stg_order_items') }} oi
        using(seller_id)
    group by s.seller_id
),


seller_late_delivery as (
    select
        oi.seller_id,
        safe_divide(
            sum(case when o.order_delivered_customer_date > o.order_estimated_delivery_date then 1 else 0 end),
            count(distinct o.order_id)
        ) as late_delivery_rate
    from {{ ref('stg_order_items') }} oi
    left join {{ ref('stg_orders') }} o
        on oi.order_id = o.order_id
    group by oi.seller_id
)

select
    sa.seller_id,
    sa.seller_zip,
    sa.seller_city,
    sa.seller_state,
    sa.num_orders,
    sa.total_revenue,
    coalesce(sld.late_delivery_rate, 0) as late_delivery_rate

from seller_agg sa
left join seller_late_delivery sld on sa.seller_id = sld.seller_id