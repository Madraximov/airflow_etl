# dbt — E-Commerce Data Warehouse transforms

This dbt project owns every transformation **downstream of the raw staging
landing tables**. Airflow extracts the CSVs and loads `staging.orders` /
`staging.customers`; dbt does the rest and tests it.

```
staging.orders / staging.customers   ← raw landing (loaded by Airflow)
        │  (dbt sources)
        ▼
staging:   stg_orders, stg_customers            (views)
        ▼
dwh:       dim_date, dim_customer, dim_product,
           dim_geography, fact_sales            (tables — star schema)
        ▼
datamart:  monthly_sales, customer_ltv,
           product_performance                  (views)
```

## Layout

| Path | Purpose |
|---|---|
| `models/staging/`        | Clean & type the raw landing tables (sources → `stg_*` views) |
| `models/marts/core/`     | Conformed dimensions + `fact_sales` (the `dwh` star schema) |
| `models/marts/datamart/` | Aggregated reporting views |
| `tests/`                 | Singular data tests (non-negative sales, fact not empty) |
| `macros/`                | `generate_schema_name` override → exact schema names |

Surrogate keys are deterministic MD5 hashes of the natural keys, so the whole
warehouse can be rebuilt idempotently with no sequence state. No external dbt
packages are required (`dbt build` works without `dbt deps`).

## Running it

The connection reads the same `DW_*` env vars as the Airflow containers, with
local-dev defaults baked into `profiles.yml`.

```bash
# from this directory, against the Postgres started by docker compose
export DW_HOST=localhost DW_PORT=5433
dbt build --project-dir . --profiles-dir .   # run models + tests
dbt docs generate --project-dir . --profiles-dir . && dbt docs serve
```

Inside the pipeline, the Airflow DAG `etl_ecommerce` runs `dbt build`
automatically as its final task.

## Data-quality tests

The original hand-coded DQ checks are now dbt tests:

| Original check | dbt test |
|---|---|
| No NULL FKs in `fact_sales` | `not_null` + `relationships` on each key in `_core__models.yml` |
| No negative sales | `tests/assert_fact_sales_non_negative.sql` |
| `fact_sales` not empty | `tests/assert_fact_sales_not_empty.sql` |

Plus `unique`/`not_null` on every dimension key and `accepted_values` on
customer segment.
