"""
E-Commerce ETL Pipeline
-----------------------
Extract/Load handled by Airflow (CSV → staging landing tables); all
transformation, the star-schema build and data-quality testing are handled by
dbt (`dbt build` runs models + tests in dependency order):

    CSV → staging → [dbt] → dwh (star schema) → datamart views

Schedule: daily at 02:00 UTC
"""
from __future__ import annotations

import csv
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DATA_DIR = Path("/opt/airflow/data")
SQL_DIR = Path("/opt/airflow/sql")
DBT_DIR = "/opt/airflow/dbt"

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


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="etl_ecommerce",
    description="E-Commerce ELT: CSV → Staging (Airflow) → DWH → Data Mart (dbt)",
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ecommerce", "etl", "dwh", "dbt"],
)
def etl_ecommerce():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # ------------------------------------------------------------------
    # STEP 1 — Initialize the raw staging landing zone
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
    # STEP 3 — Load raw CSVs into the staging landing tables
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
    # STEP 4 — Transform + test with dbt
    #   `dbt build` runs, in dependency order:
    #     staging views → dwh dims/fact (tables) → datamart views,
    #   running every schema/data test as it goes. A failed test fails the task.
    # ------------------------------------------------------------------
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt build --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"
        ),
        env={
            "DBT_PROFILES_DIR": DBT_DIR,
            "DW_HOST": os.getenv("DW_HOST", "postgres-dw"),
            "DW_PORT": os.getenv("DW_PORT", "5432"),
            "DW_DB": os.getenv("DW_DB", "ecommerce_dw"),
            "DW_USER": os.getenv("DW_USER", "dw_user"),
            "DW_PASSWORD": os.getenv("DW_PASSWORD", "dw_password"),
            "PATH": os.getenv("PATH", ""),
        },
        append_env=True,
    )

    # ------------------------------------------------------------------
    # Wire up the DAG
    # ------------------------------------------------------------------
    schema = init_schema()
    validated = extract_validate()
    stg_customers = load_staging_customers()
    stg_orders = load_staging_orders()

    (
        start
        >> schema
        >> validated
        >> [stg_customers, stg_orders]
        >> dbt_build
        >> end
    )


etl_ecommerce()
