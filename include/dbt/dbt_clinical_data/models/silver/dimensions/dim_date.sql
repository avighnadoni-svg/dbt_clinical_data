{{ config(materialized='table') }}

with date_spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="to_date('2010-01-01')",
        end_date="to_date('2035-12-31')"
    ) }}

),

final as (

    select
        to_number(to_char(date_day, 'YYYYMMDD')) as date_key,
        cast(date_day as date) as full_date,
        year(date_day) as year_number,
        quarter(date_day) as quarter_number,
        month(date_day) as month_number,
        monthname(date_day) as month_name,
        weekofyear(date_day) as week_of_year,
        day(date_day) as day_of_month,
        dayofweek(date_day) as day_of_week,
        dayname(date_day) as day_name,
        case when dayofweek(date_day) in (0,6) then 1 else 0 end as is_weekend,
        case when date_day = last_day(date_day, 'month') then 1 else 0 end as is_month_end,
        case when quarter(date_day) in (1,2,3,4)
                  and month(date_day) in (3,6,9,12)
                  and date_day = last_day(date_day, 'month')
             then 1 else 0 end as is_quarter_end,
        case when month(date_day) = 12 and day(date_day) = 31 then 1 else 0 end as is_year_end
    from date_spine

)

select * from final