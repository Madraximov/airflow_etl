-- Product dimension built from the distinct products seen in orders.
-- Grain is one row per product_id. `distinct on` deterministically resolves the
-- rare cases where two product names share a product_id (the source derives
-- product_id from a hash of the name, which can collide), mirroring the
-- last-write-wins behaviour of the original ON CONFLICT (product_id) load.
with products as (
    select distinct on (product_id)
        product_id,
        product_name,
        category,
        sub_category
    from {{ ref('stg_orders') }}
    order by product_id, product_name
)

select
    md5(product_id)             as product_key,
    product_id,
    product_name,
    category,
    sub_category,
    now()                       as valid_from,
    cast(null as timestamp)     as valid_to,
    true                        as is_current
from products
