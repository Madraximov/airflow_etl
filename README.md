# E-Commerce ETL Pipeline

End-to-end data engineering project: daily batch ELT pipeline that ingests raw e-commerce data, transforms it into a star-schema Data Warehouse with **dbt**, and exposes aggregated Data Mart views for analytics. Airflow handles extract/load; dbt owns every transformation and tests it.

## Architecture

```
CSV Files (raw)
      │  Apache Airflow — extract + load (DAG: etl_ecommerce, daily 02:00 UTC)
      ▼
┌─────────────┐
│   staging   │  raw landing tables (orders, customers)
│  (landing)  │
└─────────────┘
      │  ────────────  dbt build (models + tests)  ────────────
      ▼
┌─────────────┐
│   staging   │  stg_orders, stg_customers      (cleaned/typed views)
└─────────────┘
      ▼
┌──────────────────────────────────────┐
│       Data Warehouse — dwh (Star)    │
│  dim_date    dim_customer            │
│  dim_product dim_geography           │
│         └──► fact_sales ◄──┘        │
└──────────────────────────────────────┘
      ▼
┌─────────────┐
│  datamart   │  monthly_sales · customer_ltv · product_performance
│   (views)   │
└─────────────┘
```

The Airflow DAG loads the raw CSVs into the `staging` landing tables, then runs
`dbt build`, which constructs the cleaned staging views, the `dwh` star schema
and the `datamart` views **in dependency order** and runs every data-quality
test along the way. See [`dbt/README.md`](dbt/README.md) for the dbt project.

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.8 |
| Transformation | dbt (dbt-postgres 1.9) |
| Storage | PostgreSQL 15 |
| Language | Python 3.11 |
| Containerization | Docker Compose |
| Testing | pytest · dbt tests |

## Project Structure

```
airflow_etl/
├── dags/
│   └── etl_ecommerce.py       # Main DAG: load staging + run dbt
├── dbt/                       # dbt project — owns dwh + datamart transforms
│   ├── models/
│   │   ├── staging/           # stg_orders, stg_customers (sources → views)
│   │   └── marts/
│   │       ├── core/          # dim_*, fact_sales (star schema, tables)
│   │       └── datamart/      # monthly_sales, customer_ltv, product_performance
│   ├── tests/                 # singular data tests
│   ├── macros/                # generate_schema_name override
│   ├── dbt_project.yml
│   └── profiles.yml
├── sql/
│   └── ddl/
│       └── 01_create_staging.sql   # raw landing zone (dwh/datamart now in dbt)
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
      → dbt_build        # dbt: staging → dwh (star) → datamart + all tests
      → end
```

## Data Quality Checks

Data quality is enforced as **dbt tests**, run automatically by `dbt build`
(a failing test fails the `dbt_build` task):

- No NULL / orphaned foreign keys in `fact_sales` (`not_null` + `relationships`)
- No negative sales values (`tests/assert_fact_sales_non_negative.sql`)
- Row-count sanity / empty-table alert (`tests/assert_fact_sales_not_empty.sql`)
- `unique` + `not_null` on every dimension key; `accepted_values` on segment

Run them on their own with `dbt test` (see `dbt/README.md`).

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
# Python unit tests (data generator + helpers)
pip install pytest
pytest tests/ -v

# dbt model + data-quality tests (needs the warehouse running)
cd dbt
export DW_HOST=localhost DW_PORT=5433
dbt build --project-dir . --profiles-dir .   # build + test
dbt test  --project-dir . --profiles-dir .   # tests only
```

## Key Design Decisions

- **ELT with dbt** — extraction/loading stays in Airflow; all transformation logic lives in version-controlled, tested dbt models. Lineage, docs and tests come for free (`dbt docs serve`).
- **Idempotent rebuilds** — staging is truncated before each load; dbt rebuilds the `dwh` tables from scratch and uses deterministic hash surrogate keys, so re-runs always converge to the same result.
- **Star schema** — separates business dimensions (date, customer, product, geography) from measures for optimal query performance.
- **SCD awareness** — `is_current` / `valid_from` / `valid_to` columns on `dim_customer` and `dim_product` are ready to be converted into dbt snapshots for Type 2 history.
- **Tests as gates** — the original imperative DQ checks are now declarative dbt tests that block the pipeline on failure.
