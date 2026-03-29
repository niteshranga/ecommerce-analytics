{{
    config(
        materialized='table',
        schema='INT'
    )
}}

with source as (
    select * from {{ ref('stg_order_reviews') }}
),

deduped as (
    select *,
        row_number() over (
            partition by order_id  
            order by review_answer_timestamp desc nulls last
        ) as rn
    from source
),

final as (
    select
        order_id,
        review_score,
        review_score_label,
        review_sentiment,
        has_comment,
        review_creation_date,
        review_answer_timestamp,
        review_response_hours,
        dbt_loaded_at
    from deduped
    where rn = 1
)

select * from final