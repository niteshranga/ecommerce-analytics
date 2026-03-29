{{
    config(
        materialized='table',
        schema='FACTS'
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

payments as (
    select
        order_id,
        sum(payment_value)                              as total_payment_value,
        max(payment_installments)                       as max_installments,
        count(distinct payment_type)                    as payment_methods_used,
        boolor_agg(is_installment_purchase)             as has_installments
    from {{ ref('stg_order_payments') }}
    group by order_id
),

reviews as (
    select
        order_id,
        review_score,
        review_sentiment,
        has_comment,
        review_response_hours
    from {{ ref('int_order_reviews') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} as order_key,

        -- Natural key
        o.order_id,

        -- Foreign keys
        c.customer_key,
        to_number(to_char(o.order_purchase_date, 'YYYYMMDD')) as date_key,

        -- Order attributes
        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,

        -- Delivery metrics
        o.actual_delivery_days,
        o.estimated_delivery_days,
        o.actual_delivery_days - 
        o.estimated_delivery_days                       as delivery_delay_days,

        case
            when o.actual_delivery_days <= o.estimated_delivery_days
            then true else false
        end                                             as is_on_time,

        -- Payment metrics
        p.total_payment_value,
        p.max_installments,
        p.payment_methods_used,
        p.has_installments,

        -- Review metrics
        r.review_score,
        r.review_sentiment,
        r.has_comment,
        r.review_response_hours,

        -- Metadata
        current_timestamp()                             as dbt_loaded_at

    from orders o
    left join customers c
        on o.customer_id = c.customer_id
    left join payments p
        on o.order_id = p.order_id
    left join reviews r
        on o.order_id = r.order_id
)

select * from final