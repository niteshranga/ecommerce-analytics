{{
    config(
        materialized='view',
        schema='STG'
    )
}}

with source as (
    select * from {{ source('raw', 'orders') }}
),

cleaned as (
    select
        -- IDs
        order_id,
        customer_id,

        -- Status
        order_status,

        -- Timestamps — cast from VARCHAR to TIMESTAMP
        try_to_timestamp(order_purchase_timestamp)      as order_purchase_timestamp,
        try_to_timestamp(order_approved_at)             as order_approved_at,
        try_to_timestamp(order_delivered_carrier_date)  as order_delivered_carrier_date,
        try_to_timestamp(order_delivered_customer_date) as order_delivered_customer_date,
        try_to_timestamp(order_estimated_delivery_date) as order_estimated_delivery_date,

        -- Derived columns
        cast(order_purchase_timestamp as date)          as order_purchase_date,

        -- Delivery metrics
        datediff(
            day,
            try_to_timestamp(order_purchase_timestamp),
            try_to_timestamp(order_delivered_customer_date)
        ) as actual_delivery_days,

        datediff(
            day,
            try_to_timestamp(order_purchase_timestamp),
            try_to_timestamp(order_estimated_delivery_date)
        ) as estimated_delivery_days,

        -- Metadata
        current_timestamp() as dbt_loaded_at

    from source
    where order_id is not null
)

select * from cleaned
