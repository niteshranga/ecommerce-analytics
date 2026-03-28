{{
    config(
        materialized='view',
        schema='STG'
    )
}}

with source as (
    select * from {{ source('raw', 'order_payments') }}
),

cleaned as (
    select
        -- IDs
        order_id,

        -- Cast numerics
        try_to_number(payment_sequential)               as payment_sequential,
        try_to_number(payment_installments)             as payment_installments,
        try_to_number(payment_value, 10, 2)             as payment_value,

        -- Payment type
        lower(payment_type)                             as payment_type,

        -- Grouping
        case lower(payment_type)
            when 'credit_card'  then 'Card'
            when 'debit_card'   then 'Card'
            when 'boleto'       then 'Bank Transfer'
            when 'voucher'      then 'Voucher'
            else 'Other'
        end                                             as payment_type_group,

        -- Flag
        case
            when try_to_number(payment_installments) > 1
            then true
            else false
        end                                             as is_installment_purchase,

        -- Metadata
        current_timestamp()                             as dbt_loaded_at

    from source
    where order_id is not null
        and try_to_number(payment_value, 10, 2) >= 0
        and lower(payment_type) != 'not_defined'
)

select * from cleaned