{{
    config(
        materialized='table',
        schema='MARTS'
    )
}}

with orders as (
    select * from {{ ref('fct_orders') }}
),

order_items as (
    select * from {{ ref('fct_order_items') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

dates as (
    select * from {{ ref('dim_date') }}
),

order_metrics as (
    select
        o.order_id,
        o.customer_key,
        o.date_key,
        o.order_status,
        o.total_payment_value,
        o.actual_delivery_days,
        o.delivery_delay_days,
        o.is_on_time,
        o.review_score,
        o.review_sentiment,
        o.has_installments,

        -- Order item metrics
        count(oi.order_item_key)                        as total_items,
        sum(oi.price)                                   as total_price,
        sum(oi.freight_value)                           as total_freight,
        sum(oi.total_item_value)                        as total_item_value

    from orders o
    left join order_items oi
        on o.order_key = oi.order_key
    group by
        o.order_id, o.customer_key, o.date_key,
        o.order_status, o.total_payment_value,
        o.actual_delivery_days, o.delivery_delay_days,
        o.is_on_time, o.review_score, o.review_sentiment,
        o.has_installments
),

final as (
    select
        -- Keys
        om.order_id,
        c.customer_id,
        c.customer_city,
        c.customer_state,
        c.customer_region,

        -- Date attributes
        d.full_date                                     as order_date,
        d.month_name,
        d.month_number,
        d.quarter_name,
        d.year_number,
        d.is_weekend,

        -- Order metrics
        om.order_status,
        om.total_items,
        om.total_price,
        om.total_freight,
        om.total_item_value,
        om.total_payment_value,

        -- Delivery metrics
        om.actual_delivery_days,
        om.delivery_delay_days,
        om.is_on_time,

        -- Review metrics
        om.review_score,
        om.review_sentiment,

        -- Payment metrics
        om.has_installments,

        -- Metadata
        current_timestamp()                             as dbt_loaded_at

    from order_metrics om
    left join customers c
        on om.customer_key = c.customer_key
    left join dates d
        on om.date_key = d.date_key
        where om.total_payment_value is not null
        and om.total_payment_value > 0 
)

select * from final