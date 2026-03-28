{{
    config(
        materialized='table',
        schema='DIMS'
    )
}}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2016-01-01' as date)",
        end_date="cast('2019-12-31' as date)"
    ) }}
),

final as (
    select
        -- Surrogate key
        to_number(to_char(date_day, 'YYYYMMDD'))        as date_key,

        -- Date
        date_day                                        as full_date,

        -- Day
        dayofweek(date_day)                             as day_of_week,
        dayname(date_day)                               as day_name,
        day(date_day)                                   as day_of_month,
        dayofyear(date_day)                             as day_of_year,

        -- Week
        weekofyear(date_day)                            as week_of_year,

        -- Month
        month(date_day)                                 as month_number,
        monthname(date_day)                             as month_name,

        -- Quarter
        quarter(date_day)                               as quarter_number,
        'Q' || quarter(date_day)                        as quarter_name,

        -- Year
        year(date_day)                                  as year_number,

        -- Flags
        case when dayofweek(date_day) in (1, 7)
            then true else false
        end                                             as is_weekend,

        case when dayofweek(date_day) in (1, 7)
            then false else true
        end                                             as is_weekday

    from date_spine
)

select * from final