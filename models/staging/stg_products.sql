{{
    config(
        materialized='view',
        schema='STG'
    )
}}

with source as (
    select * from {{ source('raw', 'products') }}
),

category_translation as (
    select * from {{ source('raw', 'product_category_translation') }}
),

cleaned as (
    select
        -- IDs
        p.product_id,

        -- Category — join English translation
        p.product_category_name                         as product_category_name_pt,
        coalesce(
            ct.product_category_name_english,
            p.product_category_name,
            'uncategorized'
        )                                               as product_category_name,

        -- Physical attributes — cast from VARCHAR
        try_to_number(p.product_name_length)            as product_name_length,
        try_to_number(p.product_description_length)     as product_description_length,
        try_to_number(p.product_photos_qty)             as product_photos_qty,
        try_to_number(p.product_weight_g)               as product_weight_g,
        try_to_number(p.product_length_cm)              as product_length_cm,
        try_to_number(p.product_height_cm)              as product_height_cm,
        try_to_number(p.product_width_cm)               as product_width_cm,

        -- Derived — volumetric weight
        round(
            try_to_number(p.product_length_cm) *
            try_to_number(p.product_height_cm) *
            try_to_number(p.product_width_cm) / 1000
        , 2)                                            as product_volume_cm3,

        -- Metadata
        current_timestamp()                             as dbt_loaded_at

    from source p
    left join category_translation ct
        on p.product_category_name = ct.product_category_name

    where p.product_id is not null
)

select * from cleaned
