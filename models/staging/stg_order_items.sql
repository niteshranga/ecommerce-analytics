{{
    config(
        materialized='view',
        schema='STG'
    )
}}

with source as (
    select * from {{ source('raw', 'order_items') }}
),

cleaned as (
    select
        -- IDs
        order_id,
        product_id,
        seller_id,

        -- Cast numeric types from VARCHAR
        try_to_number(order_item_id)                    as order_item_id,
        try_to_number(price, 10, 2)                     as price,
        try_to_number(freight_value, 10, 2)             as freight_value,

        -- Derived
        try_to_number(price, 10, 2) +
        try_to_number(freight_value, 10, 2)             as total_item_value,

        -- Timestamps
        try_to_timestamp(shipping_limit_date)           as shipping_limit_date,

        -- Metadata
        current_timestamp()                             as dbt_loaded_at

    from source
    where order_id is not null
        and product_id is not null
        and try_to_number(price, 10, 2) > 0
)

select * from cleaned