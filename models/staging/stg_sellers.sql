{{
    config(
        materialized='view',
        schema='STG'
    )
}}

with source as (
    select * from {{ source('raw', 'sellers') }}
),

cleaned as (
    select
        -- IDs
        seller_id,

        -- Location
        seller_zip_code_prefix                          as zip_code,
        initcap(seller_city)                            as seller_city,
        upper(seller_state)                             as seller_state,

        -- Derived — Brazil region mapping
        case upper(seller_state)
            when 'SP' then 'Southeast'
            when 'RJ' then 'Southeast'
            when 'MG' then 'Southeast'
            when 'ES' then 'Southeast'
            when 'RS' then 'South'
            when 'SC' then 'South'
            when 'PR' then 'South'
            when 'BA' then 'Northeast'
            when 'CE' then 'Northeast'
            when 'PE' then 'Northeast'
            when 'AM' then 'North'
            when 'PA' then 'North'
            else 'Other'
        end                                             as seller_region,

        -- Metadata
        current_timestamp()                             as dbt_loaded_at

    from source
    where seller_id is not null
)

select * from cleaned
