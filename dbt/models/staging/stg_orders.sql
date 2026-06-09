-- Cleaned & typed order lines from the raw landing table. All downstream
-- dimensions and the fact table build off this model.
with source as (
    select * from {{ source('staging', 'orders') }}
),

cleaned as (
    select
        trim(order_id)                      as order_id,
        trim(customer_id)                   as customer_id,
        order_date::date                    as order_date,
        ship_date::date                     as ship_date,
        trim(ship_mode)                     as ship_mode,
        trim(product_id)                    as product_id,
        trim(product_name)                  as product_name,
        trim(category)                      as category,
        trim(sub_category)                  as sub_category,
        sales::numeric(12, 2)               as sales,
        quantity::integer                   as quantity,
        discount::numeric(5, 2)             as discount,
        profit::numeric(12, 2)              as profit,
        trim(city)                          as city,
        trim(state)                         as state,
        trim(country)                       as country,
        trim(region)                        as region
    from source
    where order_id is not null
)

select * from cleaned
