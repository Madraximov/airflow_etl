-- Staging layer: raw data landing zone
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.orders (
    order_id        VARCHAR(50),
    customer_id     VARCHAR(50),
    order_date      VARCHAR(30),
    ship_date       VARCHAR(30),
    ship_mode       VARCHAR(50),
    product_id      VARCHAR(50),
    product_name    TEXT,
    category        VARCHAR(50),
    sub_category    VARCHAR(50),
    sales           NUMERIC(12, 2),
    quantity        INTEGER,
    discount        NUMERIC(5, 2),
    profit          NUMERIC(12, 2),
    city            VARCHAR(100),
    state           VARCHAR(100),
    country         VARCHAR(100),
    region          VARCHAR(50),
    _loaded_at      TIMESTAMP DEFAULT NOW(),
    _source_file    VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id     VARCHAR(50),
    customer_name   VARCHAR(150),
    segment         VARCHAR(50),
    _loaded_at      TIMESTAMP DEFAULT NOW()
);
