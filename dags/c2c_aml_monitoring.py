"""
C2C AML Monitoring
------------------
Daily card-to-card (C2C) anti-money-laundering monitoring report.

Runs three independent ClickHouse checks over *yesterday's* transfers and
writes the result into a single Excel workbook (one sheet per check):

    1. "Cards_5plus_10M"   — phones sending to >= 5 distinct cards with a
                              daily total >= 10,000,000 UZS.
    2. "Transfers_50plus"  — phones with >= 50 successful transfers in the day.
    3. "Amount_150M_206M"  — phones with a daily total in [150M, 206M) UZS.

Output file: C2C_AML_Monitoring_{YYYY-MM-DD}.xlsx
Schedule:    08:00 Asia/Tashkent (UTC+5) -> 03:00 UTC.
"""
from __future__ import annotations

import io
import logging
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

CLICKHOUSE_CONN_ID = "tk-bpl-chdb-2"
REPORT_DIR         = "/tmp/airflow_reports"
EMAIL_RECIPIENTS   = [
    "AMadrakhimov@beeline.uz"
]

# Excel sheet names (max 31 chars) paired with a human-readable title.
SHEET_CARDS     = "Cards_5plus_10M"
SHEET_TRANSFERS = "Transfers_50plus"
SHEET_AMOUNT    = "Amount_150M_206M"

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2025, 4, 6),
    "retries": 1,
}


def get_date_range(context: dict) -> tuple[str, str]:
    """Return [day, next_day) as YYYY-MM-DD strings.

    Defaults to *yesterday* in Tashkent time (UTC+5); a manual run can
    override via the ``report_date`` param.
    """
    params          = context.get("params") or {}
    report_date_str = params.get("report_date", "").strip()
    if report_date_str:
        day = datetime.strptime(report_date_str, "%Y-%m-%d")
    else:
        day = (datetime.utcnow() + timedelta(hours=5) - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    next_day = day + timedelta(days=1)
    return day.strftime("%Y-%m-%d"), next_day.strftime("%Y-%m-%d")


def _ch_execute(conn_id: str, sql: str, fmt: str | None = None) -> str:
    conn       = BaseHook.get_connection(conn_id)
    fallback   = 9000 if conn.port == 8123 else 8123
    last_error = None
    for port in [conn.port, fallback]:
        url  = f"http://{conn.host}:{port}/" + (f"?default_format={fmt}" if fmt else "")
        resp = requests.post(url, data=sql, auth=(conn.login, conn.password))
        if resp.ok:
            return resp.text
        if "clickhouse-client" in resp.text.lower():
            last_error = resp
            continue
        raise Exception(f"ClickHouse error {resp.status_code}: {resp.text}")
    raise Exception(f"ClickHouse error {last_error.status_code}: {last_error.text}")


# ══════════════════════════════════════════════
# SQL — three C2C AML checks (all scoped to [start, end))
# ══════════════════════════════════════════════
def sql_cards_5plus_10m(start: str, end: str) -> str:
    """>= 5 distinct receiver cards AND daily total >= 10,000,000 UZS."""
    return f"""
SELECT VerificationPhoneNumber,
       toDate(tpr.CreatedDate)        AS created_day,
       groupUniqArray(cab.Name)       AS BusinessName,
       groupUniqArray(sp.Address)     AS Address,
       uniqExact(ReceiverCard)        AS NumberOf_cards,
       groupUniqArray(ReceiverCard)   AS Cards,
       count()                        AS TransactionCount,
       sum(tpr.Amount) / 100          AS TotalAmount,
       case
           when u.Id = '00000000-0000-0000-0000-000000000000' then False
           else True
       end as in_Beepul
FROM metabase_tables.TransferTransactions tt
JOIN metabase_tables.Transfers_processing_Receipts tpr
     ON toString(tpr.TId) = toString(tt.Tid)
JOIN metabase_tables.Core_Agent_Businesses cab
     ON cab.Id = tt.BusinessId
JOIN ch_business_core_agent.SalesPoints sp
     ON sp.Id = tt.SalesPointId
left join clickhouse_users.Users u
     on u.Login = tt.VerificationPhoneNumber
WHERE tpr.CreatedDate >= '{start}'
  AND tpr.CreatedDate <  '{end}'
  AND tpr.State = 4
GROUP BY VerificationPhoneNumber, created_day, in_Beepul
HAVING uniqExact(ReceiverCard) >= 5
   AND sum(tpr.Amount) / 100 >= 10000000
ORDER BY TotalAmount DESC
FORMAT TabSeparatedWithNames
"""


def sql_transfers_50plus(start: str, end: str) -> str:
    """>= 50 successful transfers in the day."""
    return f"""
SELECT VerificationPhoneNumber,
       toDate(tpr.CreatedDate)        AS created_day,
       groupUniqArray(cab.Name)       AS BusinessName,
       groupUniqArray(sp.Address)     AS Address,
       uniqExact(ReceiverCard)        AS DistinctCards,
       groupUniqArray(ReceiverCard)   AS Cards,
       count()                        AS TransferCount,
       sum(tpr.Amount) / 100          AS TotalAmount,
       case
           when u.Id = '00000000-0000-0000-0000-000000000000' then False
           else True
       end as in_Beepul
FROM metabase_tables.TransferTransactions tt
JOIN metabase_tables.Transfers_processing_Receipts tpr
     ON toString(tpr.TId) = toString(tt.Tid)
JOIN metabase_tables.Core_Agent_Businesses cab
     ON cab.Id = tt.BusinessId
JOIN ch_business_core_agent.SalesPoints sp
     ON sp.Id = tt.SalesPointId
left join clickhouse_users.Users u
     on u.Login = tt.VerificationPhoneNumber
WHERE tpr.CreatedDate >= '{start}'
  AND tpr.CreatedDate <  '{end}'
  AND tpr.State = 4
GROUP BY VerificationPhoneNumber, created_day, in_Beepul
HAVING TransferCount >= 50
ORDER BY TotalAmount DESC
FORMAT TabSeparatedWithNames
"""


def sql_amount_150m_206m(start: str, end: str) -> str:
    """Daily total in [150,000,000 ; 206,000,000) UZS."""
    return f"""
SELECT VerificationPhoneNumber,
       toDate(tpr.CreatedDate)        AS created_day,
       groupUniqArray(cab.Name)       AS BusinessName,
       groupUniqArray(sp.Address)     AS Address,
       uniqExact(ReceiverCard)        AS DistinctCards,
       groupUniqArray(ReceiverCard)   AS Cards,
       count()                        AS TransferCount,
       sum(tpr.Amount) / 100          AS TotalAmount,
       case
           when u.Id = '00000000-0000-0000-0000-000000000000' then False
           else True
       end as in_Beepul
FROM metabase_tables.TransferTransactions tt
JOIN metabase_tables.Transfers_processing_Receipts tpr
     ON toString(tpr.TId) = toString(tt.Tid)
JOIN metabase_tables.Core_Agent_Businesses cab
     ON cab.Id = tt.BusinessId
JOIN ch_business_core_agent.SalesPoints sp
     ON sp.Id = tt.SalesPointId
left join clickhouse_users.Users u
     on u.Login = tt.VerificationPhoneNumber
WHERE tpr.CreatedDate >= '{start}'
  AND tpr.CreatedDate <  '{end}'
  AND tpr.State = 4
GROUP BY VerificationPhoneNumber, created_day, in_Beepul
HAVING TotalAmount >= 150000000
   AND TotalAmount <  206000000
ORDER BY TotalAmount DESC
FORMAT TabSeparatedWithNames
"""


# ══════════════════════════════════════════════
# TASK 1 — Fetch all three checks into one workbook
# ══════════════════════════════════════════════
def fetch_c2c_aml(**context):
    start, end = get_date_range(context)
    os.makedirs(REPORT_DIR, exist_ok=True)

    checks = [
        (SHEET_CARDS,     sql_cards_5plus_10m(start, end)),
        (SHEET_TRANSFERS, sql_transfers_50plus(start, end)),
        (SHEET_AMOUNT,    sql_amount_150m_206m(start, end)),
    ]

    path = os.path.join(REPORT_DIR, f"C2C_AML_Monitoring_{start}.xlsx")
    sheet_counts: dict[str, int] = {}

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet_name, sql in checks:
            df = pd.read_csv(io.StringIO(_ch_execute(CLICKHOUSE_CONN_ID, sql)), sep="\t")
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            sheet_counts[sheet_name] = len(df)
            logging.info(f"[C2C AML] {sheet_name}: {len(df):,} rows for {start}")

    logging.info(f"[C2C AML] Saved {path}")

    ti = context["ti"]
    ti.xcom_push(key="report_path",  value=path)
    ti.xcom_push(key="report_start", value=start)
    ti.xcom_push(key="sheet_counts", value=sheet_counts)


# ══════════════════════════════════════════════
# TASK 2 — Email
# ══════════════════════════════════════════════
def send_c2c_aml_email(**context):
    ti           = context["ti"]
    path         = ti.xcom_pull(key="report_path")
    start        = ti.xcom_pull(key="report_start")
    sheet_counts = ti.xcom_pull(key="sheet_counts") or {}

    labels = {
        SHEET_CARDS:     ">= 5 cards & >= 10M UZS",
        SHEET_TRANSFERS: ">= 50 transfers",
        SHEET_AMOUNT:    "150M - 206M UZS",
    }

    rows_html = ""
    for i, (sheet, label) in enumerate(labels.items()):
        bg = "#f9f9f9" if i % 2 == 0 else "#ffffff"
        rows_html += f"""
      <tr style="background:{bg};">
        <td style="padding:8px 16px;">{label}</td>
        <td style="padding:8px 16px;text-align:right;"><b>{sheet_counts.get(sheet, 0):,}</b></td>
      </tr>"""

    html = f"""
    <h2 style="color:#2c3e50">C2C AML Monitoring — {start}</h2>

    <table style="border-collapse:collapse;font-size:14px;">
      <tr style="background:#2c3e50;color:#fff;">
        <th style="padding:8px 16px;text-align:left;">Проверка</th>
        <th style="padding:8px 16px;text-align:right;">Найдено</th>
      </tr>{rows_html}
    </table>

    <p style="color:#999;font-size:11px;margin-top:24px;">
      DAG: c2c_aml_monitoring · {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    </p>
    """

    send_email(
        to=EMAIL_RECIPIENTS,
        subject=f"C2C AML Monitoring — {start}",
        html_content=html,
        files=[path],
    )
    logging.info(f"[C2C AML] Email sent for {start}")


# ══════════════════════════════════════════════
# DAG — 08:00 Asia/Tashkent (UTC+5) = 03:00 UTC
# ══════════════════════════════════════════════
with DAG(
    "c2c_aml_monitoring",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    catchup=False,
    tags=["clickhouse", "reporting", "aml", "c2c"],
    params={
        "report_date": Param(
            default="",
            type="string",
            title="Report Date",
            description="YYYY-MM-DD. Пример: 2026-06-01. Пусто — вчерашний день.",
        )
    },
) as dag:
    t1 = PythonOperator(task_id="fetch_c2c_aml",      python_callable=fetch_c2c_aml)
    t2 = PythonOperator(task_id="send_c2c_aml_email", python_callable=send_c2c_aml_email)
    t1 >> t2
