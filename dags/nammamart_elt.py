from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.utils.trigger_rule import TriggerRule


INCLUDE_DIR = Path(__file__).resolve().parents[1] / "include"
if str(INCLUDE_DIR) not in sys.path:
    sys.path.insert(0, str(INCLUDE_DIR))

from nammamart_pipeline import (  # noqa: E402
    DATA_DIR,
    FILES,
    INCREMENT_FILE,
    run_elt,
    run_named_sql,
    write_pipeline_audit,
)


DEFAULT_ARGS = {
    "owner": "nammamart",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=45),
}


@dag(
    dag_id="nammamart_elt",
    description="Append-only raw NammaMart ELT with SQL staging and marts.",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["nammamart", "elt", "assignment"],
)
def nammamart_elt():
    @task
    def extract_orders() -> str:
        path = DATA_DIR / FILES["orders"]
        if not path.is_file():
            raise FileNotFoundError(f"Required source file is missing: {path}")
        return str(path)

    @task
    def extract_payments() -> str:
        path = DATA_DIR / FILES["payments"]
        if not path.is_file():
            raise FileNotFoundError(f"Required source file is missing: {path}")
        return str(path)

    @task
    def extract_customers() -> str:
        path = DATA_DIR / FILES["customers"]
        if not path.is_file():
            raise FileNotFoundError(f"Required source file is missing: {path}")
        return str(path)

    @task
    def extract_products() -> str:
        path = DATA_DIR / FILES["products"]
        if not path.is_file():
            raise FileNotFoundError(f"Required source file is missing: {path}")
        return str(path)

    @task
    def extract_stores() -> str:
        path = DATA_DIR / FILES["stores"]
        if not path.is_file():
            raise FileNotFoundError(f"Required source file is missing: {path}")
        return str(path)

    @task
    def extract_increment() -> str:
        path = DATA_DIR / INCREMENT_FILE
        return str(path) if path.is_file() else ""

    @task
    def load_raw_and_transform(
        orders_path: str,
        payments_path: str,
        customers_path: str,
        products_path: str,
        stores_path: str,
        increment_path: str,
    ) -> dict:
        context = get_current_context()
        result = run_elt(
            context["run_id"],
            "nammamart_elt",
            {
                "orders": orders_path,
                "payments": payments_path,
                "customers": customers_path,
                "products": products_path,
                "stores": stores_path,
                "increment": increment_path,
            },
            started_at=context["dag_run"].start_date.isoformat(),
        )
        print(
            f"[ELT] raw rows={result['rows_extracted']} "
            f"staged orders={result['rows_loaded']} "
            f"quarantined={result['rows_quarantined']}"
        )
        print(f"[ELT raw counts] {result['raw_counts']}")
        return result

    @task
    def report(result: dict) -> dict:
        kpis = run_named_sql(
            """SELECT order_date, zone, store_name, net_revenue,
                      avg_delivery_minutes, delivered_within_15_min_pct
               FROM mart_daily_store_kpis
               ORDER BY order_date, zone, store_name"""
        )
        categories = run_named_sql(
            """SELECT category, revenue, gross_margin
               FROM mart_category_margin ORDER BY category"""
        )
        payment_modes = run_named_sql(
            """SELECT payment_mode, delivered_orders, net_revenue
               FROM mart_payment_mode_revenue ORDER BY payment_mode"""
        )
        mismatches = run_named_sql(
            """SELECT order_id, reason, mismatch_amount
               FROM mart_payment_reconciliation ORDER BY order_id"""
        )
        rejected = run_named_sql(
            """SELECT file_name, failure_reason, COUNT(*)
               FROM elt_quarantine WHERE _run_id = ?
               GROUP BY file_name, failure_reason ORDER BY file_name, failure_reason""",
            [result["run_id"]],
        )
        print(f"[ELT daily zone/store revenue and SLA] {kpis}")
        print(f"[ELT category revenue/margin] {categories}")
        print(f"[ELT revenue by payment mode] {payment_modes}")
        print(f"[ELT payment mismatches] {mismatches}")
        print(f"[ELT quarantined today by file/reason] {rejected}")
        return {
            **result,
            "zone_rows": len(kpis),
            "category_rows": len(categories),
            "mismatch_rows": len(mismatches),
        }

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def audit_run() -> None:
        context = get_current_context()
        report_result = context["ti"].xcom_pull(task_ids="report")
        status = "success" if isinstance(report_result, dict) else "failed"
        metrics = context["ti"].xcom_pull(task_ids="load_raw_and_transform") or {}
        write_pipeline_audit(
            context["run_id"],
            "nammamart_elt",
            context["dag_run"].start_date.isoformat(),
            status,
            metrics,
        )

    extracted = [
        extract_orders(), extract_payments(), extract_customers(),
        extract_products(), extract_stores(), extract_increment(),
    ]
    loaded = load_raw_and_transform(*extracted)
    reported = report(loaded)
    audited = audit_run()
    audited.set_upstream(reported)


nammamart_elt()
