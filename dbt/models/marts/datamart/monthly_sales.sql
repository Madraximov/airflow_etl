-- Monthly sales performance by category & region.
select
    d.year,
    d.month,
    d.month_name,
    p.category,
    g.region,
    count(distinct f.order_id)                                      as orders_count,
    sum(f.quantity)                                                 as units_sold,
    round(sum(f.sales)::numeric, 2)                                 as total_revenue,
    round(sum(f.profit)::numeric, 2)                                as total_profit,
    round(avg(f.discount)::numeric, 4)                              as avg_discount,
    round((sum(f.profit) / nullif(sum(f.sales), 0) * 100)::numeric, 2) as profit_margin_pct
from {{ ref('fact_sales') }} f
join {{ ref('dim_date') }}      d on f.order_date_key = d.date_key
join {{ ref('dim_product') }}   p on f.product_key    = p.product_key
join {{ ref('dim_geography') }} g on f.geo_key        = g.geo_key
group by d.year, d.month, d.month_name, p.category, g.region
