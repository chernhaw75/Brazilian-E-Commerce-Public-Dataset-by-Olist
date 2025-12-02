{{ config(materialized='view') }}

with products_translated as (
    select
        product_category_name as prod_cat_name,
        product_category_name_english as translated
    from {{ source('raw_data', 'product_category_name_translation') }}
)

select * from products_translated