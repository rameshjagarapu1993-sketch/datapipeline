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
    load_etl,
    validate_etl_sources,
    write_etl_quarantine,
    write_pipeline_audit,
    warehouse_connection,
)


DEFAULT_ARGS = {
    "owner": "nammamart",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=45),
}


@dag(
    dag_id="nammamart_etl",
    description="Validated NammaMart ETL pipeline with quarantine and reconciled marts.",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["nammamart", "etl", "assignment"],
)
def nammamart_etl():
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
    def validate_sources(
        orders_path: str,
        payments_path: str,
        customers_path: str,
        products_path: str,
        stores_path: str,
        increment_path: str,
    ) -> dict:
        context = get_current_context()
        run_id = context["run_id"]
        sources = {
            "orders": orders_path,
            "payments": payments_path,
            "customers": customers_path,
            "products": products_path,
            "stores": stores_path,
            "increment": increment_path,
        }
        result = validate_etl_sources(
            run_id, "nammamart_etl", sources,
            started_at=context["dag_run"].start_date.isoformat(),
        )
        print(
            f"[ETL validate] extracted={result['rows_extracted']} "
            f"passed={result['rows_loaded']} quarantined={result['rows_quarantined']}"
        )
        return result

    @task
    def quarantine(result: dict) -> dict:
        result = write_etl_quarantine(result)
        print(f"[ETL quarantine] files={result['quarantine_files']}")
        return result

    @task
    def transform_and_load(result: dict) -> dict:
        loaded = load_etl(result)
        print(
            f"[ETL load] fact_rows={loaded['fact_rows']} "
            f"reconciliation_rows={loaded['reconciliation_rows']}"
        )
        return loaded

    @task
    def report(loaded: dict) -> dict:
        with warehouse_connection() as connection:
            zone = connection.execute(
                """SELECT order_date, zone, store_name, net_revenue
                FROM etl_daily_zone_revenue ORDER BY order_date, zone, store_name"""
            ).fetchall()
            category = connection.execute(
                """SELECT category, revenue, gross_margin
                FROM etl_category_margin ORDER BY category"""
            ).fetchall()
            rejected = connection.execute(
                """SELECT file_name, failure_reason, COUNT(*) AS rejected_rows
                FROM etl_quarantine WHERE run_id = ? AND dag_id = ?
                GROUP BY file_name, failure_reason
                ORDER BY file_name, failure_reason""",
                [loaded["run_id"], loaded["dag_id"]],
            ).fetchall()
        print(f"[ETL daily zone/store revenue] {zone}")
        print(f"[ETL category revenue/margin] {category}")
        print(f"[ETL quarantined today by file/reason] {rejected}")
        return {
            **loaded,
            "zone_rows": len(zone),
            "category_rows": len(category),
        }

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def audit_run() -> None:
        context = get_current_context()
        report_result = context["ti"].xcom_pull(task_ids="report")
        status = "success" if isinstance(report_result, dict) else "failed"
        metrics = context["ti"].xcom_pull(task_ids="transform_and_load") or {}
        validation = context["ti"].xcom_pull(task_ids="validate_sources") or {}
        metrics = {
            **validation,
            **metrics,
        }
        write_pipeline_audit(
            context["run_id"],
            "nammamart_etl",
            context["dag_run"].start_date.isoformat(),
            status,
            metrics,
        )

    extracted = [
        extract_orders(), extract_payments(), extract_customers(),
        extract_products(), extract_stores(), extract_increment(),
    ]
    validated = validate_sources(*extracted)
    quarantined = quarantine(validated)
    loaded = transform_and_load(quarantined)
    reported = report(loaded)
    audited = audit_run()
    audited.set_upstream(reported)


nammamart_etl()
