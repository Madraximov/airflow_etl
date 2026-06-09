-- Sales fact at order-line grain. Joins cleaned orders to the conformed
-- dimensions on their natural keys and carries the surrogate keys + measures.
-- A full rebuild keeps the load idempotent (re-running produces the same rows).
with orders as (
    select * from {{ ref('stg_orders') }}
),

dim_customer as (
    select customer_key, customer_id from {{ ref('dim_customer') }} where is_current
),

dim_product as (
    select product_key, product_id from {{ ref('dim_product') }} where is_current
),

dim_geography as (
    select geo_key, city, state, country from {{ ref('dim_geography') }}
)

select
    md5(o.order_id || '|' || o.product_id || '|' || o.order_date::text) as sale_key,
    o.order_id,
    cast(to_char(o.order_date, 'YYYYMMDD') as integer)  as order_date_key,
    cast(to_char(o.ship_date,  'YYYYMMDD') as integer)  as ship_date_key,
    c.customer_key,
    p.product_key,
    g.geo_key,
    o.ship_mode,
    o.quantity,
    o.sales,
    o.discount,
    o.profit,
    now() as _loaded_at
from orders o
left join dim_customer  c on c.customer_id = o.customer_id
left join dim_product   p on p.product_id  = o.product_id
left join dim_geography g on g.city = o.city
                         and g.state = o.state
                         and g.country = o.country
