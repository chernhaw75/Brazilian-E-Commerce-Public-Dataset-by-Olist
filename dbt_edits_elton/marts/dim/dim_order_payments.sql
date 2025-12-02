{{config(materialized='table')}}

WITH
payment_agg as (
    select
        order_id,
        coalesce(sum(payment_value),0) as total_payment,
        max(case when payment_type = 'credit_card' then 1 else 0 end) as payment_type_credit_card,
        max(case when payment_type = 'debit_card' then 1 else 0 end) as payment_type_debit_card,
        max(case when payment_type = 'voucher' then 1 else 0 end) as payment_type_voucher,
        max(case when payment_type = 'boleto' then 1 else 0 end) as payment_type_boleto,
        max(payment_installments) as installment_count,
        max(case when payment_installments > 1 then 1 else 0 end) as installments_used

    from {{ ref('stg_order_payments')}}
    group by order_id
)

select * from payment_agg