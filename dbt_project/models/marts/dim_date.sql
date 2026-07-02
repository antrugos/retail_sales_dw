with bounds as (
    select
        min(sale_date) as min_date,
        max(sale_date) as max_date
    from {{ ref('stg_sales') }}
),

date_spine as (
    select
        dateadd(day, seq4(), (select min_date from bounds)) as date_day
    from table(generator(rowcount => 365))
)

select 
    d.date_day,
    extract(year from d.date_day)       as year,
    extract(month from d.date_day)      as month,
    extract(dayofweek from date_day)    as day_of_week,
    case 
        when extract(dayofweek from d.date_day) in (0, 6)
        then true 
        else false 
    end as is_weekend
from date_spine d
join bounds b on d.date_day between b.min_date and b.max_date