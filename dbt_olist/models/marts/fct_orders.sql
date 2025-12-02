select
  o.order_id,
  o.customer_id,
  o.order_status,
  o.order_purchase_timestamp,
  sum(i.price + i.freight_value) as gross_order_value,
  count(distinct i.order_item_id) as items_count
from {{ ref('stg_olist_orders') }} o
left join {{ ref('stg_olist_order_items') }} i
  on o.order_id = i.order_id
group by 1,2,3,4
