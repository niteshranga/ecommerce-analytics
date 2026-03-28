{{
    config(
        materialized='table',
        schema='DIMS'
    )
}}

with source as (
    select * from {{ ref('stg_products') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,

        -- Natural key
        product_id,

        -- Attributes
        product_category_name,
        product_category_name_pt,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        product_volume_cm3,

        -- Metadata
        current_timestamp()                             as dbt_loaded_at

    from source
)

select * from final