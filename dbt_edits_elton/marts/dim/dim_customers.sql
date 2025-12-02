{{ config(materialized = 'table')}}

WITH base AS (
    select
        c.customer_id,
        c.customer_unique_id,
        c.customer_zip,
        c.customer_city,
        c.customer_state
    FROM {{ ref('stg_customers')}} c
),

orders AS (
    select
        o.customer_id,
        min(o.order_purchase_timestamp) AS first_purchase_timestamp,
        max(o.order_purchase_timestamp) AS last_purchase_timestamp,
        count (*) AS total_orders
    from {{ ref('stg_orders') }} o
    group by o.customer_id
),

customer_value AS (
    select
        oi.order_id,
        o.customer_id,
        sum(oi.price + oi.freight_value) AS order_total
    from {{ ref('stg_order_items') }} oi
    join {{ ref('stg_orders') }} o on oi.order_id = o.order_id
    group by oi.order_id, o.customer_id
),

customer_ltv AS (
    select
        customer_id,
        sum(order_total) AS total_revenue,
        avg(order_total) AS avg_order_value
    from customer_value
    group by customer_id
),

max_date AS (
    select
        date(max(order_purchase_timestamp)) as max_order_date
        from {{ ref('stg_orders' )}}
),

recency AS (
    select
        orders.customer_id,
        date_diff(md.max_order_date, date(orders.last_purchase_timestamp), DAY) AS days_since_last_purchase
    from orders
    cross join max_date md
),

avg_days_bwt_orders as (
    with orders_dates as(
        select
            customer_id,
            date(order_purchase_timestamp) as order_date,
            lag(date(order_purchase_timestamp)) over (partition by customer_id order by order_purchase_timestamp) as prev_order_date
            from {{ ref('stg_orders')}}
    ),

    gaps as (
        select
            customer_id,
            date_diff(order_date, prev_order_date, day) as days_between
        from orders_dates
        where prev_order_date is not null
    )

    select
        customer_id,
        avg(days_between) as avg_days_between_orders,
        stddev_pop(days_between) as stddev_days_between_orders,
        count(1) as num_gaps
    
    from gaps
    group by customer_id
)

select
    b.customer_id,
    b.customer_unique_id,
    b.customer_zip,
    b.customer_city,
    b.customer_state,

    o.first_purchase_timestamp,
    o.last_purchase_timestamp,
    o.total_orders,
    
    coalesce(adbo.avg_days_between_orders,0) as avg_days_between_orders,
    coalesce(adbo.stddev_days_between_orders,0) as stddev_days_between_orders,

    coalesce(c.total_revenue,0) as total_revenue,
    coalesce(c.avg_order_value,0) as avg_order_value,

    r.days_since_last_purchase

from base b
left join orders o on b.customer_id = o.customer_id
left join customer_ltv c on b.customer_id = c.customer_id
left join recency r on b.customer_id = r.customer_id
left join avg_days_bwt_orders adbo on b.customer_id = adbo.customer_id