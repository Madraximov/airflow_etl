-- Data Warehouse: star schema
CREATE SCHEMA IF NOT EXISTS dwh;

-- Dimension: Date
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key        INTEGER PRIMARY KEY,  -- YYYYMMDD
    full_date       DATE NOT NULL,
    year            INTEGER,
    quarter         INTEGER,
    month           INTEGER,
    month_name      VARCHAR(20),
    week            INTEGER,
    day_of_week     INTEGER,
    day_name        VARCHAR(20),
    is_weekend      BOOLEAN
);

-- Dimension: Customer
CREATE TABLE IF NOT EXISTS dwh.dim_customer (
    customer_key    SERIAL PRIMARY KEY,
    customer_id     VARCHAR(50) UNIQUE NOT NULL,
    customer_name   VARCHAR(150),
    segment         VARCHAR(50),
    valid_from      TIMESTAMP DEFAULT NOW(),
    valid_to        TIMESTAMP,
    is_current      BOOLEAN DEFAULT TRUE
);

-- Dimension: Product
CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_key     SERIAL PRIMARY KEY,
    product_id      VARCHAR(50) UNIQUE NOT NULL,
    product_name    TEXT,
    category        VARCHAR(50),
    sub_category    VARCHAR(50),
    valid_from      TIMESTAMP DEFAULT NOW(),
    valid_to        TIMESTAMP,
    is_current      BOOLEAN DEFAULT TRUE
);

-- Dimension: Geography
CREATE TABLE IF NOT EXISTS dwh.dim_geography (
    geo_key         SERIAL PRIMARY KEY,
    city            VARCHAR(100),
    state           VARCHAR(100),
    country         VARCHAR(100),
    region          VARCHAR(50),
    UNIQUE (city, state, country)
);

-- Fact: Sales
CREATE TABLE IF NOT EXISTS dwh.fact_sales (
    sale_key        SERIAL PRIMARY KEY,
    order_id        VARCHAR(50),
    order_date_key  INTEGER REFERENCES dwh.dim_date(date_key),
    ship_date_key   INTEGER REFERENCES dwh.dim_date(date_key),
    customer_key    INTEGER REFERENCES dwh.dim_customer(customer_key),
    product_key     INTEGER REFERENCES dwh.dim_product(product_key),
    geo_key         INTEGER REFERENCES dwh.dim_geography(geo_key),
    ship_mode       VARCHAR(50),
    quantity        INTEGER,
    sales           NUMERIC(12, 2),
    discount        NUMERIC(5, 2),
    profit          NUMERIC(12, 2),
    _loaded_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_order_date ON dwh.fact_sales(order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer   ON dwh.fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product    ON dwh.fact_sales(product_key);
