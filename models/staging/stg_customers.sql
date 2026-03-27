{{
    config(
        materialized='view',
        schema='STG'
    )
}}

with source as (
    select * from {{ source('raw', 'customers') }}
),

cleaned as (
    select
        -- IDs
        customer_id,
        customer_unique_id,

        -- Location
        customer_zip_code_prefix                        as zip_code,
        initcap(customer_city)                          as customer_city,
        upper(customer_state)                           as customer_state,

        -- Derived — Brazil region mapping
        case upper(customer_state)
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
        end                                             as customer_region,

        -- Metadata
        current_timestamp()                             as dbt_loaded_at

    from source
    where customer_id is not null
)

select * from cleaned

