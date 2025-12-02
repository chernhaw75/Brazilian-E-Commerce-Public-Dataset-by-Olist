{{ config (materialized = 'table') }}

WITH order_items_agg AS (
    select
        order_id,
        count(*) as num_items,
        sum(price + freight_value) as total_order_value,
        sum(freight_value) as total_freight
    from {{ ref('stg_order_items')}}
    group by order_id
),

seller_stats as (
    select
        order_id,
        count(distinct seller_id) as seller_count,
        case when count(distinct seller_id) > 1 then 1 else 0 end as multiple_sellers
    from {{ ref('stg_order_items')}}
    group by order_id
),

primary_seller as (
    select
        order_id,
        seller_id as primary_seller_id
    from (
        select
            order_id,
            seller_id,
            sum(price + freight_value) as seller_revenue,
            row_number() over (partition by order_id order by sum(price + freight_value) desc) as rn
        from {{ ref('stg_order_items') }}
        group by order_id, seller_id
    ) 
    where rn = 1
),

orders_flags as (
    select
        order_id,
        case when order_delivered_customer_date > order_estimated_delivery_date then 1 else 0 end as late_delivery_flag
    from {{ ref('stg_orders')}}
)

select
    o.order_id,
    o.customer_id,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    coalesce(oi.num_items,0) as num_items,
    coalesce(oi.total_order_value,0) as total_order_value,
    coalesce(oi.total_freight,0) as total_freight,

    coalesce(ss.seller_count,0) as seller_count,
    cast(coalesce(ss.multiple_sellers,0) as int64) as multiple_sellers,

    ps.primary_seller_id,

    cast(ofl.late_delivery_flag as int64) as late_delivery_flag

from {{ ref('stg_orders') }} o
left join order_items_agg oi on o.order_id = oi.order_id
left join seller_stats ss on o.order_id = ss.order_id
left join primary_seller ps on o.order_id = ps.order_id
left join orders_flags ofl on o.order_id = ofl.order_id