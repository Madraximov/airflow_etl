# E-Commerce ETL Pipeline

End-to-end data engineering project: daily batch ETL pipeline that ingests raw e-commerce data, transforms it into a star-schema Data Warehouse, and exposes aggregated Data Mart views for analytics.

## Architecture

```
CSV Files (raw)
      │
      ▼
┌─────────────┐     Apache Airflow (scheduler + webserver)
│   Staging   │  ◄──────────────────────────────────────────
│  (landing)  │     DAG: etl_ecommerce — runs daily 02:00 UTC
└─────────────┘
      │
      ▼
┌──────────────────────────────────────┐
│          Data Warehouse (Star Schema)│
│                                      │
│  dim_date    dim_customer            │
│  dim_product dim_geography           │
│         └──► fact_sales ◄──┘        │
└──────────────────────────────────────┘
      │
      ▼
┌─────────────┐
│  Data Mart  │  monthly_sales · customer_ltv · product_performance
│   (views)   │
└─────────────┘
```

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.8 |
| Storage | PostgreSQL 15 |
| Language | Python 3.11 |
| Containerization | Docker Compose |
| Testing | pytest |

## Project Structure

```
airflow_etl/
├── dags/
│   └── etl_ecommerce.py       # Main DAG (TaskFlow API)
├── sql/
│   └── ddl/
│       ├── 01_create_staging.sql
│       ├── 02_create_dwh.sql  # Star schema
│       └── 03_create_datamart.sql
├── scripts/
│   └── generate_data.py       # Synthetic dataset generator (5 000 orders)
├── plugins/
│   └── db_hook.py             # DB utilities
├── tests/
│   └── test_transformations.py
├── data/raw/                  # Source CSV files (git-ignored)
├── config/
│   └── airflow.cfg.example
├── docker-compose.yml
└── requirements.txt
```

## Quick Start

```bash
# 1. Clone & enter project
git clone https://github.com/madraximov/airflow_etl.git
cd airflow_etl

# 2. Generate synthetic data
python scripts/generate_data.py
# → data/raw/orders.csv    (5 000 rows)
# → data/raw/customers.csv (  200 rows)

# 3. Start all services
docker compose up -d

# 4. Open Airflow UI → http://localhost:8080
#    Login: admin / admin
#    Trigger DAG: etl_ecommerce

# 5. Query the Data Mart (PostgreSQL on port 5433)
psql -h localhost -p 5433 -U dw_user -d ecommerce_dw \
  -c "SELECT * FROM datamart.monthly_sales LIMIT 10;"
```

## DAG Tasks

```
start → init_schema → extract_validate
      → load_staging_customers
      → load_staging_orders
      → load_dim_date
      → load_dim_customer
      → load_dim_product
      → load_dim_geography
      → load_fact_sales
      → data_quality_checks
      → end
```

## Data Quality Checks

The pipeline includes automated checks after every load:

- No NULL foreign keys in `fact_sales`
- No negative sales values
- Row-count sanity (empty table alert)

## Data Mart Queries

```sql
-- Monthly revenue by region
SELECT year, month, region, total_revenue, profit_margin_pct
FROM datamart.monthly_sales
ORDER BY year, month, total_revenue DESC;

-- Top 10 customers by lifetime value
SELECT customer_name, segment, total_orders, lifetime_revenue
FROM datamart.customer_ltv
ORDER BY lifetime_revenue DESC
LIMIT 10;

-- Best-selling product categories
SELECT category, sub_category, units_sold, total_revenue
FROM datamart.product_performance
ORDER BY total_revenue DESC;
```

## Running Tests

```bash
pip install pytest
pytest tests/ -v
```

## Key Design Decisions

- **Idempotent loads** — staging tables are truncated before each load; fact table deletes today's batch before re-inserting, so re-runs are safe.
- **Star schema** — separates business dimensions (date, customer, product, geography) from measures for optimal query performance.
- **SCD awareness** — `is_current` / `valid_from` / `valid_to` columns on `dim_customer` and `dim_product` are ready for Type 2 slowly-changing dimension logic.
- **TaskFlow API** — Airflow 2.x decorator syntax keeps DAG code clean and testable.
