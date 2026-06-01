"""
E-Commerce ETL Pipeline
-----------------------
Full ELT: CSV → Staging → DWH (Star Schema) → Data Mart views
Schedule: daily at 02:00 UTC
"""
from __future__ import annotations

import csv
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

sys.path.insert(0, "/opt/airflow/plugins")

DATA_DIR = Path("/opt/airflow/data")
SQL_DIR = Path("/opt/airflow/sql")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_conn():
    import psycopg2

    return psycopg2.connect(
        host=os.getenv("DW_HOST", "postgres-dw"),
        port=int(os.getenv("DW_PORT", 5432)),
        dbname=os.getenv("DW_DB", "ecommerce_dw"),
        user=os.getenv("DW_USER", "dw_user"),
        password=os.getenv("DW_PASSWORD", "dw_password"),
    )


def _run_sql_file(path: Path) -> None:
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(path.read_text())
        conn.commit()
    finally:
        conn.close()


def _to_int_date(val: str) -> int | None:
    try:
        return int(datetime.strptime(val, "%Y-%m-%d").strftime("%Y%m%d"))
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="etl_ecommerce",
    description="E-Commerce ELT: CSV → Staging → DWH → Data Mart",
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ecommerce", "etl", "dwh"],
)
def etl_ecommerce():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # ------------------------------------------------------------------
    # STEP 1 — Initialize schema
    # ------------------------------------------------------------------
    @task
    def init_schema():
        for ddl_file in sorted((SQL_DIR / "ddl").glob("*.sql")):
            print(f"Running DDL: {ddl_file.name}")
            _run_sql_file(ddl_file)

    # ------------------------------------------------------------------
    # STEP 2 — Extract: validate & count source files
    # ------------------------------------------------------------------
    @task
    def extract_validate() -> dict:
        stats = {}
        for fname in ("orders.csv", "customers.csv"):
            path = DATA_DIR / "raw" / fname
            if not path.exists():
                raise FileNotFoundError(f"Source file not found: {path}")
            with open(path, encoding="utf-8") as f:
                rows = sum(1 for _ in f) - 1  # minus header
            stats[fname] = rows
            print(f"{fname}: {rows} rows")
        return stats

    # ------------------------------------------------------------------
    # STEP 3 — Load staging
    # ------------------------------------------------------------------
    @task
    def load_staging_customers():
        from psycopg2.extras import execute_values

        path = DATA_DIR / "raw" / "customers.csv"
        conn = _get_conn()
        try:
            with open(path, encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            data = [(r["customer_id"], r["customer_name"], r["segment"]) for r in rows]
            with conn.cursor() as cur:
                cur.execute("TRUNCATE staging.customers")
                execute_values(
                    cur,
                    "INSERT INTO staging.customers (customer_id, customer_name, segment) VALUES %s",
                    data,
                )
            conn.commit()
            print(f"Loaded {len(data)} customers into staging")
        finally:
            conn.close()

    @task
    def load_staging_orders():
        from psycopg2.extras import execute_values

        path = DATA_DIR / "raw" / "orders.csv"
        conn = _get_conn()
        try:
            with open(path, encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            data = [
                (
                    r["order_id"], r["customer_id"], r["order_date"], r["ship_date"],
                    r["ship_mode"], r["product_id"], r["product_name"],
                    r["category"], r["sub_category"],
                    float(r["sales"]), int(r["quantity"]), float(r["discount"]),
                    float(r["profit"]), r["city"], r["state"], r["country"], r["region"],
                    "orders.csv",
                )
                for r in rows
            ]
            cols = (
                "order_id,customer_id,order_date,ship_date,ship_mode,"
                "product_id,product_name,category,sub_category,"
                "sales,quantity,discount,profit,city,state,country,region,_source_file"
            )
            with conn.cursor() as cur:
                cur.execute("TRUNCATE staging.orders")
                execute_values(
                    cur,
                    f"INSERT INTO staging.orders ({cols}) VALUES %s",
                    data,
                )
            conn.commit()
            print(f"Loaded {len(data)} orders into staging")
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # STEP 4 — Transform & Load dimensions (SCD Type 2 for customers/products)
    # ------------------------------------------------------------------
    @task
    def load_dim_date():
        conn = _get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT MIN(order_date::date), MAX(ship_date::date) FROM staging.orders")
                min_d, max_d = cur.fetchone()
            if min_d is None:
                return

            rows = []
            d = min_d
            while d <= max_d:
                rows.append((
                    int(d.strftime("%Y%m%d")), d,
                    d.year, (d.month - 1) // 3 + 1, d.month,
                    d.strftime("%B"), d.isocalendar()[1],
                    d.weekday(), d.strftime("%A"),
                    d.weekday() >= 5,
                ))
                d += timedelta(days=1)

            from psycopg2.extras import execute_values
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO dwh.dim_date
                      (date_key, full_date, year, quarter, month, month_name,
                       week, day_of_week, day_name, is_weekend)
                    VALUES %s
                    ON CONFLICT (date_key) DO NOTHING
                    """,
                    rows,
                )
            conn.commit()
            print(f"Loaded {len(rows)} date dimension rows")
        finally:
            conn.close()

    @task
    def load_dim_customer():
        conn = _get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dwh.dim_customer (customer_id, customer_name, segment)
                    SELECT DISTINCT customer_id, customer_name, segment
                    FROM staging.customers
                    ON CONFLICT (customer_id)
                    DO UPDATE SET
                        customer_name = EXCLUDED.customer_name,
                        segment       = EXCLUDED.segment
                """)
            conn.commit()
            print("Dimension customers updated")
        finally:
            conn.close()

    @task
    def load_dim_product():
        conn = _get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dwh.dim_product (product_id, product_name, category, sub_category)
                    SELECT DISTINCT product_id, product_name, category, sub_category
                    FROM staging.orders
                    ON CONFLICT (product_id)
                    DO UPDATE SET
                        product_name = EXCLUDED.product_name,
                        category     = EXCLUDED.category,
                        sub_category = EXCLUDED.sub_category
                """)
            conn.commit()
            print("Dimension products updated")
        finally:
            conn.close()

    @task
    def load_dim_geography():
        conn = _get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dwh.dim_geography (city, state, country, region)
                    SELECT DISTINCT city, state, country, region
                    FROM staging.orders
                    ON CONFLICT (city, state, country) DO NOTHING
                """)
            conn.commit()
            print("Dimension geography updated")
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # STEP 5 — Load fact table
    # ------------------------------------------------------------------
    @task
    def load_fact_sales():
        conn = _get_conn()
        try:
            with conn.cursor() as cur:
                # Avoid duplicates on reload: delete today's batch by _loaded_at
                cur.execute("DELETE FROM dwh.fact_sales WHERE _loaded_at::date = CURRENT_DATE")
                cur.execute("""
                    INSERT INTO dwh.fact_sales
                      (order_id, order_date_key, ship_date_key,
                       customer_key, product_key, geo_key,
                       ship_mode, quantity, sales, discount, profit)
                    SELECT
                        o.order_id,
                        o.order_date::date::text::integer * 0 +  -- computed below
                        CAST(TO_CHAR(o.order_date::date, 'YYYYMMDD') AS INTEGER),
                        CAST(TO_CHAR(o.ship_date::date, 'YYYYMMDD') AS INTEGER),
                        c.customer_key,
                        p.product_key,
                        g.geo_key,
                        o.ship_mode,
                        o.quantity,
                        o.sales,
                        o.discount,
                        o.profit
                    FROM staging.orders o
                    JOIN dwh.dim_customer  c ON c.customer_id = o.customer_id  AND c.is_current
                    JOIN dwh.dim_product   p ON p.product_id  = o.product_id   AND p.is_current
                    JOIN dwh.dim_geography g ON g.city = o.city AND g.state = o.state
                                            AND g.country = o.country
                """)
            conn.commit()
            print("Fact sales loaded")
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # STEP 6 — Data quality checks
    # ------------------------------------------------------------------
    @task
    def data_quality_checks():
        conn = _get_conn()
        checks_failed = []
        try:
            with conn.cursor() as cur:
                # Check 1: no nulls in fact keys
                cur.execute("""
                    SELECT COUNT(*) FROM dwh.fact_sales
                    WHERE order_date_key IS NULL OR customer_key IS NULL OR product_key IS NULL
                """)
                null_keys = cur.fetchone()[0]
                if null_keys > 0:
                    checks_failed.append(f"Null FK keys in fact_sales: {null_keys} rows")

                # Check 2: negative sales
                cur.execute("SELECT COUNT(*) FROM dwh.fact_sales WHERE sales < 0")
                neg_sales = cur.fetchone()[0]
                if neg_sales > 0:
                    checks_failed.append(f"Negative sales: {neg_sales} rows")

                # Check 3: row count sanity
                cur.execute("SELECT COUNT(*) FROM dwh.fact_sales")
                total = cur.fetchone()[0]
                print(f"Total fact_sales rows: {total}")
                if total == 0:
                    checks_failed.append("fact_sales is empty!")

            if checks_failed:
                raise ValueError("DQ failures:\n" + "\n".join(checks_failed))
            print("All data quality checks passed.")
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Wire up the DAG
    # ------------------------------------------------------------------
    schema = init_schema()
    validated = extract_validate()
    stg_customers = load_staging_customers()
    stg_orders = load_staging_orders()
    dim_date = load_dim_date()
    dim_customer = load_dim_customer()
    dim_product = load_dim_product()
    dim_geo = load_dim_geography()
    fact = load_fact_sales()
    dq = data_quality_checks()

    (
        start
        >> schema
        >> validated
        >> [stg_customers, stg_orders]
        >> [dim_date, dim_customer, dim_product, dim_geo]
        >> fact
        >> dq
        >> end
    )


etl_ecommerce()
