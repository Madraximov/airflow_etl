-- Top products by revenue.
select
    p.category,
    p.sub_category,
    p.product_name,
    count(distinct f.order_id)              as order_count,
    sum(f.quantity)                         as units_sold,
    round(sum(f.sales)::numeric, 2)         as total_revenue,
    round(sum(f.profit)::numeric, 2)        as total_profit,
    round(avg(f.discount)::numeric, 4)      as avg_discount,
    round((sum(f.profit) / nullif(sum(f.sales), 0) * 100)::numeric, 2) as profit_margin_pct
from {{ ref('fact_sales') }} f
join {{ ref('dim_product') }} p on f.product_key = p.product_key
where p.is_current
group by p.category, p.sub_category, p.product_name
