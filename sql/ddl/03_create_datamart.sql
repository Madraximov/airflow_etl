-- Data Mart: aggregated views for reporting
CREATE SCHEMA IF NOT EXISTS datamart;

-- Monthly sales performance
CREATE OR REPLACE VIEW datamart.monthly_sales AS
SELECT
    d.year,
    d.month,
    d.month_name,
    p.category,
    g.region,
    COUNT(DISTINCT f.order_id)          AS orders_count,
    SUM(f.quantity)                     AS units_sold,
    ROUND(SUM(f.sales)::NUMERIC, 2)     AS total_revenue,
    ROUND(SUM(f.profit)::NUMERIC, 2)    AS total_profit,
    ROUND(AVG(f.discount)::NUMERIC, 4)  AS avg_discount,
    ROUND((SUM(f.profit) / NULLIF(SUM(f.sales), 0) * 100)::NUMERIC, 2) AS profit_margin_pct
FROM dwh.fact_sales f
JOIN dwh.dim_date     d ON f.order_date_key = d.date_key
JOIN dwh.dim_product  p ON f.product_key    = p.product_key
JOIN dwh.dim_geography g ON f.geo_key       = g.geo_key
GROUP BY d.year, d.month, d.month_name, p.category, g.region;

-- Customer lifetime value
CREATE OR REPLACE VIEW datamart.customer_ltv AS
SELECT
    c.customer_id,
    c.customer_name,
    c.segment,
    COUNT(DISTINCT f.order_id)              AS total_orders,
    SUM(f.quantity)                         AS total_units,
    ROUND(SUM(f.sales)::NUMERIC, 2)         AS lifetime_revenue,
    ROUND(SUM(f.profit)::NUMERIC, 2)        AS lifetime_profit,
    MIN(d.full_date)                        AS first_order_date,
    MAX(d.full_date)                        AS last_order_date,
    MAX(d.full_date) - MIN(d.full_date)     AS customer_tenure_days
FROM dwh.fact_sales f
JOIN dwh.dim_customer c ON f.customer_key    = c.customer_key
JOIN dwh.dim_date     d ON f.order_date_key  = d.date_key
WHERE c.is_current = TRUE
GROUP BY c.customer_id, c.customer_name, c.segment;

-- Top products by revenue
CREATE OR REPLACE VIEW datamart.product_performance AS
SELECT
    p.category,
    p.sub_category,
    p.product_name,
    COUNT(DISTINCT f.order_id)              AS order_count,
    SUM(f.quantity)                         AS units_sold,
    ROUND(SUM(f.sales)::NUMERIC, 2)         AS total_revenue,
    ROUND(SUM(f.profit)::NUMERIC, 2)        AS total_profit,
    ROUND(AVG(f.discount)::NUMERIC, 4)      AS avg_discount,
    ROUND((SUM(f.profit) / NULLIF(SUM(f.sales), 0) * 100)::NUMERIC, 2) AS profit_margin_pct
FROM dwh.fact_sales f
JOIN dwh.dim_product p ON f.product_key = p.product_key
WHERE p.is_current = TRUE
GROUP BY p.category, p.sub_category, p.product_name;
