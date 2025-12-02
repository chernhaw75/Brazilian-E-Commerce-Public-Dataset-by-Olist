{{ config(materialized='view') }}

with reviews as (
    select
        review_id,
        order_id,
        cast(review_score as int) as review_score,
        review_comment_title,
        review_comment_message,
        cast(NULLIF(review_creation_date,'') as TIMESTAMP) as review_creation_date,
        cast(NULLIF(review_answer_timestamp,'') as TIMESTAMP) as review_answer_timestamp
    from {{ source('raw_data', 'olist_order_reviews') }}
)

select * from reviews