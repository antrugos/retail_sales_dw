with source as (
    select * from {{ source('raw_retail', 'sales_raw') }}
),
cleaned as (
    select
        sale_id,
        cast(sale_date as date)        as sale_date,
        cast(store_id as integer)      as store_id,
        cast(product_id as integer)    as product_id,
        cast(quantity as integer)      as quantity,
        cast(unit_price as number(12,2))   as unit_price,
        cast(quantity * unit_price as number(12,2)) as total_amount
    from source
)

select * from cleaned