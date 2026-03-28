{{
    config(
        materialized='table',
        schema='DIMS'
    )
}}

with source as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,

        -- Natural key
        customer_id,
        customer_unique_id,

        -- Attributes
        customer_city,
        customer_state,
        customer_region,
        zip_code,

        -- Metadata
        current_timestamp()                             as dbt_loaded_at

    from source
)

select * from final