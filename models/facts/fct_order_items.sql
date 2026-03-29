{{
    config(
        materialized='table',
        schema='FACTS'
    )
}}

with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select
        order_id,
        order_key,
        date_key
    from {{ ref('fct_orders') }}
),

products as (
    select * from {{ ref('dim_products') }}
),

sellers as (
    select * from {{ ref('dim_sellers') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['oi.order_id', 'oi.order_item_id']) }} as order_item_key,

        -- Foreign keys
        o.order_key,
        o.date_key,
        p.product_key,
        s.seller_key,

        -- Natural keys
        oi.order_id,
        oi.order_item_id,

        -- Measures
        oi.price,
        oi.freight_value,
        oi.total_item_value,

        -- Shipping
        oi.shipping_limit_date,

        -- Metadata
        current_timestamp()                             as dbt_loaded_at

    from order_items oi
    left join orders o
        on oi.order_id = o.order_id
    left join products p
        on oi.product_id = p.product_id
    left join sellers s
        on oi.seller_id = s.seller_id
)

select * from final