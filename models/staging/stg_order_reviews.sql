{{
    config(
        materialized='view',
        schema='STG'
    )
}}

with source as (
    select * from {{ source('raw', 'order_reviews') }}
),

cleaned as (
    select
        -- IDs
        review_id,
        order_id,

        -- Review score — cast from VARCHAR
        try_to_number(review_score)                     as review_score,

        -- Score grouping
        case try_to_number(review_score)
            when 5 then 'Excellent'
            when 4 then 'Good'
            when 3 then 'Neutral'
            when 2 then 'Poor'
            when 1 then 'Very Poor'
            else 'Unknown'
        end                                             as review_score_label,

        -- Sentiment flag
        case
            when try_to_number(review_score) >= 4 then 'Positive'
            when try_to_number(review_score) = 3  then 'Neutral'
            when try_to_number(review_score) <= 2 then 'Negative'
            else 'Unknown'
        end                                             as review_sentiment,

        -- Has written review flag
        case
            when review_comment_message is not null
            and length(trim(review_comment_message)) > 0
            then true
            else false
        end                                             as has_comment,

        -- Timestamps
        try_to_timestamp(review_creation_date)          as review_creation_date,
        try_to_timestamp(review_answer_timestamp)       as review_answer_timestamp,

        -- Response time in hours
        datediff(
            hour,
            try_to_timestamp(review_creation_date),
            try_to_timestamp(review_answer_timestamp)
        )                                               as review_response_hours,

        -- Metadata
        current_timestamp()                             as dbt_loaded_at

    from source
    where review_id is not null
        and order_id is not null
        and try_to_number(review_score) between 1 and 5
)

select * from cleaned
g_order_reviews.sql
