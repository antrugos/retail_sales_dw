with source as (
    select *
    from {{ source('raw_retail', 'stores_raw') }}
),
cleaned as (
    select
        cast(store_id as integer)   as store_id,
        trim(store_name)            as store_name,
        trim(city)                  as city
    from source
)
select * from cleaned