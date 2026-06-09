-- Customer lifetime value.
select
    c.customer_id,
    c.customer_name,
    c.segment,
    count(distinct f.order_id)              as total_orders,
    sum(f.quantity)                         as total_units,
    round(sum(f.sales)::numeric, 2)         as lifetime_revenue,
    round(sum(f.profit)::numeric, 2)        as lifetime_profit,
    min(d.full_date)                        as first_order_date,
    max(d.full_date)                        as last_order_date,
    max(d.full_date) - min(d.full_date)     as customer_tenure_days
from {{ ref('fact_sales') }} f
join {{ ref('dim_customer') }} c on f.customer_key   = c.customer_key
join {{ ref('dim_date') }}     d on f.order_date_key = d.date_key
where c.is_current
group by c.customer_id, c.customer_name, c.segment
