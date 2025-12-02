select
  c.customer_id,
  c.customer_unique_id,
  c.customer_city,
  c.customer_state,
  count(distinct o.order_id) as orders_count,
  min(o.order_purchase_timestamp) as first_order_date,
  max(o.order_purchase_timestamp) as last_order_date
from {{ ref('stg_olist_customers') }} c
left join {{ ref('stg_olist_orders') }} o
  on c.customer_id = o.customer_id
group by 1,2,3,4
