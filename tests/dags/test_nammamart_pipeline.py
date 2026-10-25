from __future__ import annotations

import csv
import shutil
import sys
from pathlib import Path

import pandas as pd
import pytest

INCLUDE_DIR = Path(__file__).resolve().parents[2] / "include"
if str(INCLUDE_DIR) not in sys.path:
    sys.path.insert(0, str(INCLUDE_DIR))

import nammamart_pipeline as pipeline


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_DIR = PROJECT_ROOT / "include" / "data" / "nammamart"


def copy_sources(directory: Path) -> dict[str, str]:
    directory.mkdir(parents=True, exist_ok=True)
    paths = {
        name.removesuffix(".csv").replace("_2026-10-01_increment", "_increment"): str(
            directory / name
        )
        for name in pipeline.FILES.values()
    }
    paths["increment"] = str(directory / pipeline.INCREMENT_FILE)
    for name in [*pipeline.FILES.values(), pipeline.INCREMENT_FILE]:
        shutil.copy2(SOURCE_DIR / name, directory / name)
    paths["orders"] = str(directory / "orders.csv")
    paths["payments"] = str(directory / "payments.csv")
    paths["customers"] = str(directory / "customers.csv")
    paths["products"] = str(directory / "products.csv")
    paths["stores"] = str(directory / "stores.csv")
    paths["increment"] = str(directory / pipeline.INCREMENT_FILE)
    return paths


def use_temporary_warehouse(monkeypatch: pytest.MonkeyPatch, directory: Path) -> None:
    monkeypatch.setattr(pipeline, "WAREHOUSE_PATH", directory / "warehouse.duckdb")
    monkeypatch.setattr(pipeline, "WORK_DIR", directory / "work")
    monkeypatch.setattr(pipeline, "QUARANTINE_DIR", directory / "quarantine")
    monkeypatch.setattr(pipeline, "DATA_DIR", SOURCE_DIR)


def test_source_profile_matches_assignment_counts() -> None:
    profile = pipeline.profile_csvs()
    assert {
        name: profile[name]["rows"]
        for name in (
            "orders", "payments", "customers", "products", "stores", "orders_increment"
        )
    } == {
        "orders": 2412,
        "payments": 2228,
        "customers": 405,
        "products": 152,
        "stores": 15,
        "orders_increment": 160,
    }
    assert profile["orders"]["nulls"]["customer_id"] == 18
    assert profile["customers"]["nulls"]["city"] == 6


def test_etl_critical_failures_are_quarantined_with_reasons(tmp_path: Path) -> None:
    paths = copy_sources(tmp_path / "sources")
    orders_path = Path(paths["orders"])
    frame = pd.read_csv(orders_path, dtype=str, keep_default_na=False)
    frame.loc[0, "customer_id"] = ""
    frame.loc[1, "quantity"] = "-2"
    frame.loc[2, "store_id"] = "UNKNOWN_STORE"
    frame.loc[3, "order_status"] = "SHIPPED"
    frame.loc[4, "order_ts"] = "2027-01-01 12:00:00"
    frame.to_csv(orders_path, index=False)

    result = pipeline.validate_etl_sources("synthetic", "test", paths)
    failed = pd.read_csv(result["failed_paths"]["orders"], dtype=str)
    reasons = ";".join(failed["failure_reason"])
    assert len(failed) >= 5
    assert "customer_fk_exists" in reasons or "required_fields" in reasons
    assert "quantity_positive" in reasons
    assert "store_fk_exists" in reasons
    assert "order_status_allowed" in reasons
    assert "date_not_future" in reasons


def test_etl_and_elt_are_idempotent_and_incremental(tmp_path: Path, monkeypatch) -> None:
    sources = copy_sources(tmp_path / "sources")
    use_temporary_warehouse(monkeypatch, tmp_path)

    etl_result = pipeline.validate_etl_sources("etl-run-1", "nammamart_etl", sources)
    pipeline.write_etl_quarantine(etl_result)
    first_etl = pipeline.load_etl(etl_result)
    first_count, first_revenue = pipeline.run_named_sql(
        "SELECT COUNT(*), ROUND(SUM(net_revenue), 2) FROM etl_fact_orders"
    )[0]
    pipeline.load_etl(etl_result)
    second_count, second_revenue = pipeline.run_named_sql(
        "SELECT COUNT(*), ROUND(SUM(net_revenue), 2) FROM etl_fact_orders"
    )[0]
    assert first_etl["fact_rows"] == first_count
    assert (second_count, second_revenue) == (first_count, first_revenue)

    first_elt = pipeline.run_elt("elt-run-1", "nammamart_elt", sources)
    pipeline.run_elt("elt-run-2", "nammamart_elt", sources)
    with pipeline.warehouse_connection() as connection:
        raw_count = connection.execute("SELECT COUNT(*) FROM raw_orders").fetchone()[0]
        staged_count = connection.execute(
            "SELECT COUNT(*) FROM stg_orders WHERE dq_status = 'PASS'"
        ).fetchone()[0]
        mart_total = connection.execute(
            "SELECT ROUND(SUM(net_revenue), 2) FROM mart_daily_store_kpis"
        ).fetchone()[0]
        etl_total = connection.execute(
            "SELECT ROUND(SUM(net_revenue), 2) FROM etl_fact_orders"
        ).fetchone()[0]
        increment_updates = connection.execute(
            """SELECT COUNT(*) FROM stg_orders
               WHERE order_id IN (
                   SELECT order_id FROM raw_orders
                   WHERE _source_file = 'orders_2026-10-01_increment.csv'
               )"""
        ).fetchone()[0]
        pii_columns = connection.execute(
            """SELECT column_name FROM information_schema.columns
               WHERE table_name IN (
                   'mart_top_customers', 'mart_daily_store_kpis', 'mart_category_margin'
               )
               AND column_name IN ('email', 'phone', 'full_name')"""
        ).fetchall()
        quarantine_count = connection.execute(
            "SELECT COUNT(*) FROM elt_quarantine WHERE _run_id = 'elt-run-1'"
        ).fetchone()[0]
        etl_mismatches = connection.execute(
            "SELECT COUNT(*) FROM etl_payment_reconciliation"
        ).fetchone()[0]
        elt_mismatches = connection.execute(
            "SELECT COUNT(*) FROM mart_payment_reconciliation"
        ).fetchone()[0]
        etl_categories = connection.execute(
            """SELECT category, revenue, gross_margin FROM etl_category_margin
               ORDER BY category"""
        ).fetchall()
        elt_categories = connection.execute(
            """SELECT category, revenue, gross_margin FROM mart_category_margin
               ORDER BY category"""
        ).fetchall()
        etl_quarantine_count = connection.execute(
            "SELECT COUNT(*) FROM etl_quarantine WHERE run_id = 'etl-run-1'"
        ).fetchone()[0]
    assert first_elt["raw_counts"]["orders"] == 2412
    assert raw_count == 2 * (2412 + 160)
    assert staged_count == first_elt["fact_rows"]
    assert increment_updates >= 150
    assert mart_total == etl_total
    assert etl_mismatches == elt_mismatches
    assert etl_categories == elt_categories
    assert pii_columns == []
    assert quarantine_count > 0
    assert etl_quarantine_count > 0
    assert pipeline.run_named_sql(
        "SELECT COUNT(*) FROM dq_results WHERE dag_id = 'nammamart_elt'"
    )[0][0] >= 12


def test_elt_schema_change_fails_loudly(tmp_path: Path, monkeypatch) -> None:
    sources = copy_sources(tmp_path / "sources")
    use_temporary_warehouse(monkeypatch, tmp_path)
    pipeline.run_elt("schema-run-1", "nammamart_elt", sources)

    order_path = Path(sources["orders"])
    with order_path.open(newline="", encoding="utf-8") as source_file:
        reader = csv.DictReader(source_file)
        rows = list(reader)
        fields = list(reader.fieldnames or [])
    fields[fields.index("order_amount")] = "total"
    for row in rows:
        row["total"] = row.pop("order_amount")
    with order_path.open("w", newline="", encoding="utf-8") as source_file:
        writer = csv.DictWriter(source_file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    with pytest.raises(ValueError, match="Schema change detected"):
        pipeline.run_elt("schema-run-2", "nammamart_elt", sources)


def test_volume_drift_freshness_and_missing_file_checks(
    tmp_path: Path, monkeypatch
) -> None:
    sources = copy_sources(tmp_path / "sources")
    use_temporary_warehouse(monkeypatch, tmp_path)
    with pipeline.warehouse_connection() as connection:
        pipeline.ensure_audit_tables(connection)
        connection.execute(
            """INSERT INTO dq_results VALUES (
                'previous', 'nammamart_etl', 'orders.volume_baseline',
                'Completeness', 'WARNING', 2412, 0, 'PASS', CURRENT_TIMESTAMP
            )"""
        )

    orders_path = Path(sources["orders"])
    frame = pd.read_csv(orders_path, dtype=str, keep_default_na=False).head(50)
    frame["order_ts"] = "2026-09-01 12:00:00"
    store_id = frame.loc[0, "store_id"]
    store_rows = frame["store_id"].eq(store_id)
    frame.loc[store_rows, "unit_price"] = (
        pd.to_numeric(frame.loc[store_rows, "unit_price"]) * 100
    ).astype(str)
    frame.to_csv(orders_path, index=False)

    result = pipeline.validate_etl_sources("anomaly", "nammamart_etl", sources)
    checks = {item["check_name"]: item for item in result["checks"]}
    assert checks["orders.volume_baseline"]["rows_failed"] == 1
    assert checks["orders.price_distribution_by_store"]["rows_failed"] >= 1
    assert checks["orders.latest_record_within_1_day"]["rows_failed"] == 1

    missing_sources = dict(sources)
    missing_sources["orders"] = str(tmp_path / "missing_orders.csv")
    with pytest.raises(FileNotFoundError, match="Required source file is missing"):
        pipeline.validate_etl_sources("missing", "nammamart_etl", missing_sources)
