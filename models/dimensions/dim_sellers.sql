{{
    config(
        materialized='table',
        schema='DIMS'
    )
}}

with source as (
    select * from {{ ref('stg_sellers') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['seller_id']) }} as seller_key,

        -- Natural key
        seller_id,

        -- Attributes
        seller_city,
        seller_state,
        seller_region,
        zip_code,

        -- Metadata
        current_timestamp()                             as dbt_loaded_at

    from source
)

select * from final