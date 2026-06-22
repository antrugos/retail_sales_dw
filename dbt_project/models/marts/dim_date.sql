with bounds as (
    select
        min(sale_date) as min_date,
        max(sale_date) as max_date
    from {{ ref('stg_sales') }}
),

spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="(select min_date from bounds)",
        end_date="(select max_date from bounds)"
    ) }}
)

select 
    cast(date_day as date)              as date_day,
    extract(year from date_day)         as year,
    extract(month from date_day)        as month,
    extract(dayofweek from date_day)    as day,
    case 
        when extract(dayofweek from date_day) in (0, 6)
        then true 
        else false 
    end as is_weekend
from spine