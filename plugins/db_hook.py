"""PostgreSQL hook using environment variables (no Airflow Connection required for local dev)."""
import os
import psycopg2
from psycopg2.extras import execute_values


def get_dw_connection():
    return psycopg2.connect(
        host=os.getenv("DW_HOST", "localhost"),
        port=int(os.getenv("DW_PORT", 5433)),
        dbname=os.getenv("DW_DB", "ecommerce_dw"),
        user=os.getenv("DW_USER", "dw_user"),
        password=os.getenv("DW_PASSWORD", "dw_password"),
    )


def bulk_insert(conn, table: str, rows: list[dict], page_size: int = 1000) -> int:
    if not rows:
        return 0
    cols = list(rows[0].keys())
    values = [tuple(r[c] for c in cols) in rows]  # noqa — fixed below
    values = [tuple(r[c] for c in cols) for r in rows]
    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s"
    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=page_size)
    conn.commit()
    return len(rows)


def upsert(conn, table: str, rows: list[dict], conflict_cols: list[str], page_size: int = 500) -> int:
    if not rows:
        return 0
    cols = list(rows[0].keys())
    update_cols = [c for c in cols if c not in conflict_cols]
    values = [tuple(r[c] for c in cols) for r in rows]

    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    sql = (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
        f"ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE SET {update_set}"
    )
    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=page_size)
    conn.commit()
    return len(rows)
