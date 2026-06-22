with source as (
    select * from {{ source('raw_retail', 'products_raw') }}
),
cleaned as (
    select
        cast(product_id as integer)         as product_id,
        trim(product_name)                  as product_name,
        trim(category)                      as category,
        cast(unit_price as number(12,2))    as price
    from source
)

select * from cleaned