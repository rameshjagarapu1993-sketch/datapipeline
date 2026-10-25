from __future__ import annotations

import csv
import json
import os
import re
import shutil
import sys
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import duckdb
import pandas as pd


PROJECT_DIR = Path(__file__).resolve().parent.parent
INCLUDE_DIR = PROJECT_DIR / "include"
DATA_DIR = INCLUDE_DIR / "data" / "nammamart"
SQL_DIR = INCLUDE_DIR / "sql"
WAREHOUSE_PATH = INCLUDE_DIR / "warehouse_nammamart.duckdb"
QUARANTINE_DIR = INCLUDE_DIR / "quarantine" / "nammamart"
WORK_DIR = INCLUDE_DIR / "work" / "nammamart"

FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
}
INCREMENT_FILE = "orders_2026-10-01_increment.csv"
ORDER_COLUMNS = [
    "order_id", "order_ts", "customer_id", "store_id", "product_id",
    "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode",
    "order_status", "channel", "delivery_minutes",
]
VALID_MODES = {"UPI", "Card", "Cash on Delivery", "Wallet"}
MODE_ALIASES = {
    "upi": "UPI",
    "card": "Card",
    "wallet": "Wallet",
    "cash on delivery": "Cash on Delivery",
}
VALID_STATUSES = {"DELIVERED", "CANCELLED", "RETURNED"}
VALID_PAYMENT_STATUSES = {"SUCCESS", "FAILED", "REFUNDED"}
VALID_ZONES = {"NORTH", "SOUTH", "EAST", "WEST"}


def safe_run_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", run_id).strip("_")[:120] or "manual"


@contextmanager
def warehouse_connection() -> Iterator[duckdb.DuckDBPyConnection]:
    WAREHOUSE_PATH.parent.mkdir(parents=True, exist_ok=True)
    lock_path = WAREHOUSE_PATH.with_suffix(".lock")
    with lock_path.open("a+b") as lock_file:
        if os.name == "nt":
            import msvcrt

            lock_file.seek(0)
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl

            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        connection = duckdb.connect(str(WAREHOUSE_PATH))
        try:
            yield connection
        finally:
            connection.close()
            if os.name == "nt":
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def ensure_audit_tables(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS dq_results (
            run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR,
            severity VARCHAR, rows_checked BIGINT, rows_failed BIGINT,
            status VARCHAR, checked_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP,
            rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT,
            status VARCHAR
        );
        """
    )


def _normalise(df: pd.DataFrame, table: str) -> pd.DataFrame:
    result = df.copy()
    for column in result.columns:
        if result[column].dtype == object:
            result[column] = result[column].fillna("").astype(str).str.strip()
    if "payment_mode" in result:
        result["payment_mode"] = result["payment_mode"].map(
            lambda value: MODE_ALIASES.get(value.casefold(), value)
        )
    if "order_status" in result:
        result["order_status"] = result["order_status"].str.upper()
    if "payment_status" in result:
        result["payment_status"] = result["payment_status"].str.upper()
    if "zone" in result:
        result["zone"] = result["zone"].str.title()
    if "loyalty_tier" in result:
        result["loyalty_tier"] = result["loyalty_tier"].str.title()
    if table == "products" and "category" in result:
        result["category"] = result["category"].str.strip()
    return result


def _read_source(filename: str) -> pd.DataFrame:
    path = DATA_DIR / filename
    if not path.is_file():
        raise FileNotFoundError(f"Required source file is missing: {path}")
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def _check(
    output: list[dict[str, Any]],
    name: str,
    dimension: str,
    severity: str,
    failed: pd.Series | int,
    rows_checked: int,
) -> None:
    count = int(failed.sum()) if isinstance(failed, pd.Series) else int(failed)
    output.append(
        {
            "check_name": name,
            "dimension": dimension,
            "severity": severity,
            "rows_checked": rows_checked,
            "rows_failed": count,
            "status": (
                "FAIL" if count and severity == "CRITICAL"
                else "WARN" if count
                else "PASS"
            ),
        }
    )


def _validation(
    source: pd.DataFrame,
    table: str,
    checks: list[dict[str, Any]],
    *,
    customers: set[str] | None = None,
    products: set[str] | None = None,
    stores: set[str] | None = None,
    orders: set[str] | None = None,
    order_amounts: dict[str, float] | None = None,
    now: pd.Timestamp | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    original = source.copy()
    df = _normalise(source, table)
    size = len(df)
    reasons = pd.Series("", index=df.index, dtype="object")

    def record(name: str, dimension: str, severity: str, mask: pd.Series) -> None:
        mask = mask.fillna(False).astype(bool)
        _check(checks, f"{table}.{name}", dimension, severity, mask, size)
        if severity == "CRITICAL" and mask.any():
            reason = name
            reasons.loc[mask] = reasons.loc[mask].map(
                lambda previous: f"{previous}; {reason}" if previous else reason
            )

    keys = {
        "orders": "order_id", "payments": "payment_id",
        "customers": "customer_id", "products": "product_id", "stores": "store_id",
    }
    canonical_table = "orders" if table == "orders_increment" else table
    key = keys[canonical_table]
    required = {
        "orders": ["order_id", "order_ts", "customer_id", "store_id", "product_id",
                   "quantity", "unit_price", "discount_pct", "order_amount",
                   "payment_mode", "order_status"],
        "payments": ["payment_id", "order_id", "payment_ts", "payment_mode",
                     "amount_paid", "payment_status"],
        "customers": ["customer_id", "full_name", "signup_date", "loyalty_tier"],
        "products": ["product_id", "product_name", "category", "mrp", "cost_price"],
        "stores": ["store_id", "store_name", "zone", "capacity_orders_per_day"],
    }[canonical_table]
    missing = pd.Series(False, index=df.index)
    for column in required:
        if column in df:
            missing |= df[column].eq("")
        else:
            raise ValueError(f"{table} is missing required column {column!r}")
    record("required_fields", "Completeness", "CRITICAL", missing)

    key_missing = df[key].eq("") if key in df else pd.Series(True, index=df.index)
    record("key_present", "Completeness", "CRITICAL", key_missing)
    duplicate = df[key].ne("") & df[key].duplicated(keep="first")
    record("unique_key", "Uniqueness", "CRITICAL", duplicate)

    date_columns = {
        "orders": ["order_ts"], "payments": ["payment_ts"],
        "customers": ["signup_date"], "products": ["launch_date"],
        "stores": ["open_date"],
    }[canonical_table]
    parsed_dates: dict[str, pd.Series] = {}
    for column in date_columns:
        if column in df:
            parsed = pd.to_datetime(df[column], errors="coerce", utc=True)
            parsed_dates[column] = parsed
            record(f"{column}_parseable", "Validity", "CRITICAL",
                   df[column].ne("") & parsed.isna())

    if canonical_table in {"orders", "payments"}:
        mode_bad = ~df["payment_mode"].isin(VALID_MODES) & df["payment_mode"].ne("")
        record("payment_mode_allowed", "Validity", "CRITICAL", mode_bad)
    if canonical_table == "orders":
        status_bad = ~df["order_status"].isin(VALID_STATUSES)
        record("order_status_allowed", "Validity", "CRITICAL", status_bad)
        numeric = {
            name: pd.to_numeric(df[name], errors="coerce")
            for name in ["quantity", "unit_price", "discount_pct", "order_amount"]
        }
        for name, values in numeric.items():
            record(f"{name}_numeric", "Validity", "CRITICAL",
                   values.isna() | df[name].eq(""))
        record("quantity_positive", "Accuracy", "CRITICAL", numeric["quantity"].le(0))
        record("unit_price_positive", "Accuracy", "CRITICAL", numeric["unit_price"].le(0))
        record("discount_in_range", "Accuracy", "CRITICAL",
               (numeric["discount_pct"] < 0) | (numeric["discount_pct"] > 100))
        expected = (
            numeric["quantity"] * numeric["unit_price"]
            * (1 - numeric["discount_pct"] / 100)
        ).round(2)
        record("amount_matches_formula", "Accuracy", "CRITICAL",
               (numeric["order_amount"] - expected).abs().gt(0.01))
        delivery = pd.to_numeric(df["delivery_minutes"], errors="coerce")
        record("delivered_time_present", "Completeness", "CRITICAL",
               df["order_status"].eq("DELIVERED") & delivery.isna())
        record("delivery_time_below_120", "Accuracy", "CRITICAL", delivery.ge(120))
        record("delivery_time_nonnegative", "Accuracy", "CRITICAL", delivery.lt(0))
        _check(
            checks,
            "orders.delivery_time_outside_normal_8_35",
            "Accuracy",
            "WARNING",
            (delivery.lt(8) | delivery.gt(35)).fillna(False),
            size,
        )
        if customers is not None:
            record("customer_fk_exists", "Consistency", "CRITICAL",
                   ~df["customer_id"].isin(customers))
        if products is not None:
            record("product_fk_exists", "Consistency", "CRITICAL",
                   ~df["product_id"].isin(products))
        if stores is not None:
            record("store_fk_exists", "Consistency", "CRITICAL",
                   ~df["store_id"].isin(stores))
    elif canonical_table == "payments":
        payment_status_bad = ~df["payment_status"].isin(VALID_PAYMENT_STATUSES)
        record("payment_status_allowed", "Validity", "CRITICAL", payment_status_bad)
        amount = pd.to_numeric(df["amount_paid"], errors="coerce")
        record("amount_numeric", "Validity", "CRITICAL", amount.isna())
        record("amount_nonnegative", "Accuracy", "CRITICAL", amount.lt(0))
        if orders is not None:
            record("order_fk_exists", "Consistency", "CRITICAL", ~df["order_id"].isin(orders))
        if order_amounts is not None:
            expected_amount = df["order_id"].map(order_amounts)
            successful = df["payment_status"].eq("SUCCESS")
            record(
                "success_amount_matches_order",
                "Consistency",
                "CRITICAL",
                successful & expected_amount.notna()
                & (amount - expected_amount).abs().gt(0.01),
            )
        record("gateway_ref_present", "Completeness", "WARNING", df["gateway_ref"].eq(""))
    elif canonical_table == "customers":
        tier_bad = ~df["loyalty_tier"].isin({"Bronze", "Silver", "Gold", "Platinum"})
        record("loyalty_tier_allowed", "Validity", "CRITICAL", tier_bad)
        record("email_format", "Validity", "WARNING",
               df["email"].ne("") & ~df["email"].str.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"))
        record("phone_10_digits", "Validity", "WARNING",
               df["phone"].ne("") & ~df["phone"].str.fullmatch(r"\d{10}"))
        record("pincode_6_digits", "Validity", "WARNING",
               df["pincode"].ne("") & ~df["pincode"].str.fullmatch(r"\d{6}"))
        record("city_present", "Completeness", "WARNING", df["city"].eq(""))
    elif canonical_table == "products":
        numeric = {name: pd.to_numeric(df[name], errors="coerce")
                   for name in ["mrp", "cost_price"]}
        for name, values in numeric.items():
            record(f"{name}_numeric", "Validity", "CRITICAL", values.isna())
        record("mrp_positive", "Accuracy", "CRITICAL", numeric["mrp"].le(0))
        record("cost_price_positive", "Accuracy", "CRITICAL", numeric["cost_price"].le(0))
        record("cost_not_above_mrp", "Accuracy", "CRITICAL",
               numeric["cost_price"].gt(numeric["mrp"]))
    elif canonical_table == "stores":
        record("zone_allowed", "Validity", "CRITICAL",
               ~df["zone"].str.upper().isin(VALID_ZONES))

    ref_time = now if now is not None else pd.Timestamp.now(tz="UTC")
    future = pd.Series(False, index=df.index)
    for parsed in parsed_dates.values():
        future |= parsed.gt(ref_time)
    record("date_not_future", "Timeliness", "CRITICAL", future)

    if canonical_table == "customers":
        email_present = df["email"].ne("")
        phone_present = df["phone"].ne("")
        _check(checks, "customers.pii_quality_warnings", "Validity", "WARNING",
               int((~email_present).sum() + (~phone_present).sum()), size * 2)

    failed_mask = reasons.ne("")
    passed = df.loc[~failed_mask].copy()
    failed = original.loc[failed_mask].copy()
    if len(failed):
        failed["failure_reason"] = reasons.loc[failed_mask].values
    return passed, failed


def validate_etl_sources(
    run_id: str,
    dag_id: str,
    source_paths: dict[str, str],
    started_at: str | None = None,
) -> dict[str, Any]:
    safe_id = safe_run_id(run_id)
    run_dir = WORK_DIR / safe_id
    run_dir.mkdir(parents=True, exist_ok=True)
    checks: list[dict[str, Any]] = []
    raw: dict[str, pd.DataFrame] = {}
    for table, filename in FILES.items():
        path = Path(source_paths.get(table, DATA_DIR / filename))
        if not path.is_file():
            raise FileNotFoundError(f"Required source file is missing: {path}")
        raw[table] = pd.read_csv(path, dtype=str, keep_default_na=False)

    customers_df = _normalise(raw["customers"], "customers")
    products_df = _normalise(raw["products"], "products")
    stores_df = _normalise(raw["stores"], "stores")
    customers, bad_customers = _validation(raw["customers"], "customers", checks)
    products, bad_products = _validation(raw["products"], "products", checks)
    stores, bad_stores = _validation(raw["stores"], "stores", checks)

    clean_ids = {
        "customers": set(customers["customer_id"]),
        "products": set(products["product_id"]),
        "stores": set(stores["store_id"]),
    }
    orders_clean, bad_orders = _validation(
        raw["orders"], "orders", checks,
        customers=clean_ids["customers"], products=clean_ids["products"],
        stores=clean_ids["stores"],
    )
    orders_clean["_source_file"] = FILES["orders"]
    orders_clean["_source_row"] = orders_clean.index + 2
    payment_order_ids = set(orders_clean["order_id"])
    order_amounts = dict(zip(
        orders_clean["order_id"],
        pd.to_numeric(orders_clean["order_amount"], errors="coerce"),
    ))
    payments_clean, bad_payments = _validation(
        raw["payments"], "payments", checks, orders=payment_order_ids,
        order_amounts=order_amounts,
    )

    inc_path = Path(source_paths.get("increment", DATA_DIR / INCREMENT_FILE))
    if inc_path.is_file():
        raw_increment = pd.read_csv(inc_path, dtype=str, keep_default_na=False)
        increment_clean, bad_increment = _validation(
            raw_increment, "orders_increment", checks,
            customers=clean_ids["customers"], products=clean_ids["products"],
            stores=clean_ids["stores"],
        )
        increment_clean["_source_file"] = INCREMENT_FILE
        increment_clean["_source_row"] = increment_clean.index + 2
        incremental_sources = [(inc_path, increment_clean, bad_increment)]
    else:
        incremental_sources = []

    # Incremental source rows intentionally supersede matching base order IDs.
    for column in ["order_id", "order_ts"]:
        if column not in orders_clean.columns:
            raise ValueError(f"orders.csv is missing required column {column!r}")
    files = [
        ("customers", customers, bad_customers),
        ("products", products, bad_products),
        ("stores", stores, bad_stores),
        ("orders", orders_clean, bad_orders),
        ("payments", payments_clean, bad_payments),
    ]
    for table, clean, failed in files:
        clean_path = run_dir / f"{table}_passed.csv"
        failed_path = run_dir / f"{table}_failed.csv"
        clean.to_csv(clean_path, index=False)
        failed.to_csv(failed_path, index=False)
    increment_paths: list[dict[str, str]] = []
    for path, clean, failed in incremental_sources:
        clean_path = run_dir / "orders_increment_passed.csv"
        failed_path = run_dir / "orders_increment_failed.csv"
        clean.to_csv(clean_path, index=False)
        failed.to_csv(failed_path, index=False)
        increment_paths.append(
            {"clean": str(clean_path), "failed": str(failed_path), "source": str(path)}
        )

    for table, _, failed in files:
        checks.append(
            {
                "check_name": f"{table}.critical_rows_quarantined",
                "dimension": "Completeness",
                "severity": "CRITICAL",
                "rows_checked": len(raw[table]),
                "rows_failed": len(failed),
                "status": "FAIL" if len(failed) else "PASS",
            }
        )
    latest_dates = pd.to_datetime(raw["orders"]["order_ts"], errors="coerce", utc=True)
    newest = latest_dates.max()
    stale = int(pd.isna(newest) or (pd.Timestamp.now(tz="UTC") - newest).days > 1)
    _check(checks, "orders.latest_record_within_1_day", "Timeliness", "WARNING",
           stale, 1)
    current_order_count = len(raw["orders"]) + sum(
        len(increment) for _, increment, _ in incremental_sources
    )
    baseline_count = 0
    if WAREHOUSE_PATH.exists():
        with warehouse_connection() as connection:
            existing = connection.execute(
                """SELECT COUNT(*) FROM information_schema.tables
                   WHERE table_name = 'dq_results'"""
            ).fetchone()[0]
            if existing:
                baseline_count = connection.execute(
                    """SELECT rows_checked FROM dq_results
                       WHERE check_name = 'orders.volume_baseline'
                       ORDER BY checked_at DESC LIMIT 1"""
                ).fetchone()[0] if connection.execute(
                    """SELECT COUNT(*) FROM dq_results
                       WHERE check_name = 'orders.volume_baseline'"""
                ).fetchone()[0] else 0
    volume_drop = int(
        baseline_count > 0 and current_order_count < baseline_count * 0.1
    )
    _check(checks, "orders.volume_drop_90_percent", "Completeness", "WARNING",
           volume_drop, current_order_count)
    checks[-1]["check_name"] = "orders.volume_baseline"
    order_distribution = _normalise(raw["orders"], "orders")
    prices = pd.to_numeric(order_distribution["unit_price"], errors="coerce")
    distribution = pd.DataFrame(
        {"store_id": order_distribution["store_id"], "unit_price": prices}
    ).dropna()
    global_median = float(distribution["unit_price"].median()) if len(distribution) else 0
    store_medians = distribution.groupby("store_id")["unit_price"].median()
    drifted = int(
        ((store_medians > global_median * 10) | (store_medians < global_median / 10)).sum()
        if global_median else 0
    )
    _check(checks, "orders.price_distribution_by_store", "Accuracy", "WARNING",
           drifted, len(store_medians))

    return {
        "run_id": run_id,
        "dag_id": dag_id,
        "started_at": started_at or datetime.now(timezone.utc).isoformat(),
        "work_dir": str(run_dir),
        "clean_paths": {
            table: str(run_dir / f"{table}_passed.csv") for table, _, _ in files
        },
        "failed_paths": {
            table: str(run_dir / f"{table}_failed.csv") for table, _, _ in files
        },
        "increment_paths": increment_paths,
        "checks": checks,
        "rows_extracted": sum(map(len, raw.values()))
        + sum(len(pd.read_csv(item["source"], dtype=str)) for item in increment_paths),
        "rows_quarantined": sum(len(failed) for _, _, failed in files)
        + sum(len(pd.read_csv(item["failed"], dtype=str)) for item in increment_paths),
        "rows_loaded": sum(len(clean) for _, clean, _ in files)
        + sum(len(pd.read_csv(item["clean"], dtype=str)) for item in increment_paths),
    }


def write_etl_quarantine(result: dict[str, Any]) -> dict[str, Any]:
    run_dir = QUARANTINE_DIR / safe_run_id(result["run_id"])
    run_dir.mkdir(parents=True, exist_ok=True)
    paths: list[str] = []
    for table, failed_path in result["failed_paths"].items():
        failed = pd.read_csv(failed_path, dtype=str, keep_default_na=False)
        if not failed.empty:
            destination = run_dir / f"{table}_failed_{safe_run_id(result['run_id'])}.csv"
            shutil.copy2(failed_path, destination)
            paths.append(str(destination))
    for item in result["increment_paths"]:
        failed = pd.read_csv(item["failed"], dtype=str, keep_default_na=False)
        if not failed.empty:
            destination = run_dir / f"orders_increment_failed_{safe_run_id(result['run_id'])}.csv"
            shutil.copy2(item["failed"], destination)
            paths.append(str(destination))
    result["quarantine_files"] = paths
    return result


def _read_passed(path: str) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def _mask_email(value: str) -> str:
    if "@" not in value:
        return "masked"
    local, domain = value.split("@", 1)
    return f"{local[:2]}***@{domain}"


def _mask_phone(value: str) -> str:
    digits = re.sub(r"\D", "", value)
    return f"{digits[:2]}******{digits[-2:]}" if len(digits) >= 4 else "masked"


def _number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _insert_checks(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    dag_id: str,
    checks: list[dict[str, Any]],
) -> None:
    ensure_audit_tables(connection)
    connection.execute("DELETE FROM dq_results WHERE run_id = ? AND dag_id = ?",
                       [run_id, dag_id])
    checked_at = datetime.now(timezone.utc).replace(tzinfo=None)
    connection.executemany(
        """INSERT INTO dq_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            (
                run_id, dag_id, item["check_name"], item["dimension"],
                item["severity"], int(item["rows_checked"]), int(item["rows_failed"]),
                item["status"], checked_at,
            )
            for item in checks
        ],
    )


def load_etl(result: dict[str, Any]) -> dict[str, Any]:
    customers = _read_passed(result["clean_paths"]["customers"])
    products = _read_passed(result["clean_paths"]["products"])
    stores = _read_passed(result["clean_paths"]["stores"])
    orders = _read_passed(result["clean_paths"]["orders"])
    payments = _read_passed(result["clean_paths"]["payments"])
    for item in result["increment_paths"]:
        increment = _read_passed(item["clean"])
        orders = pd.concat([orders, increment], ignore_index=True)
    if not orders.empty:
        orders = orders.drop_duplicates(subset=["order_id"], keep="last")

    for frame in (orders, payments, customers, products, stores):
        frame.columns = [str(column).strip() for column in frame.columns]
    for table, frame in (
        ("orders", orders), ("payments", payments), ("customers", customers),
        ("products", products), ("stores", stores),
    ):
        if table == "orders":
            expected = set(ORDER_COLUMNS)
            if not expected.issubset(frame.columns):
                raise ValueError(f"orders is missing columns: {sorted(expected - set(frame.columns))}")

    customers["masked_name"] = customers["customer_id"].map(lambda value: f"Customer {value}")
    customers["masked_email"] = customers["email"].map(_mask_email)
    customers["masked_phone"] = customers["phone"].map(_mask_phone)
    products["mrp"] = _number(products["mrp"])
    products["cost_price"] = _number(products["cost_price"])
    stores["capacity_orders_per_day"] = _number(stores["capacity_orders_per_day"])

    if orders.empty:
        fact = pd.DataFrame(columns=[
            "order_id", "order_ts", "order_date", "customer_id", "store_id",
            "store_name", "zone", "product_id", "product_name", "category",
            "quantity", "unit_price", "discount_pct", "order_amount", "cost_price",
            "payment_mode", "order_status", "delivery_minutes", "net_revenue",
            "gross_margin", "within_15_min", "customer_name", "masked_email",
            "masked_phone", "loyalty_tier", "_source_file", "_source_row",
        ])
    else:
        for column in ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"]:
            orders[column] = _number(orders[column])
        enriched = orders.merge(
            customers[["customer_id", "masked_name", "masked_email", "masked_phone",
                       "loyalty_tier"]],
            on="customer_id", how="inner", validate="many_to_one",
        ).merge(
            products[["product_id", "product_name", "category", "cost_price"]],
            on="product_id", how="inner", validate="many_to_one",
        ).merge(
            stores[["store_id", "store_name", "zone"]],
            on="store_id", how="inner", validate="many_to_one",
        )
        enriched["order_date"] = pd.to_datetime(
            enriched["order_ts"], errors="coerce"
        ).dt.date.astype(str)
        enriched["net_revenue"] = enriched["order_amount"].where(
            enriched["order_status"].eq("DELIVERED"), 0.0
        )
        enriched["gross_margin"] = (
            enriched["net_revenue"]
            - enriched["quantity"] * enriched["cost_price"]
        ).round(2)
        enriched["within_15_min"] = enriched["delivery_minutes"].le(15)
        fact = enriched.rename(
            columns={"masked_name": "customer_name"}
        )[
            [
                "order_id", "order_ts", "order_date", "customer_id", "store_id",
                "store_name", "zone", "product_id", "product_name", "category",
                "quantity", "unit_price", "discount_pct", "order_amount", "cost_price",
                "payment_mode", "order_status", "delivery_minutes", "net_revenue",
                "gross_margin", "within_15_min", "customer_name", "masked_email",
                "masked_phone", "loyalty_tier", "_source_file", "_source_row",
            ]
        ]

    payments["amount_paid"] = _number(payments["amount_paid"])
    raw_payments = _read_source(FILES["payments"])
    raw_payments = _normalise(raw_payments, "payments")
    raw_payments["amount_paid"] = _number(raw_payments["amount_paid"])
    reconciliation = _reconcile(orders, raw_payments)
    quarantine_records: list[dict[str, str]] = []
    key_columns = {
        "customers": "customer_id",
        "products": "product_id",
        "stores": "store_id",
        "orders": "order_id",
        "payments": "payment_id",
    }
    for table, failed_path in result["failed_paths"].items():
        failed = _read_passed(failed_path)
        source_file = FILES[table]
        for row in failed.to_dict(orient="records"):
            quarantine_records.append({
                "run_id": result["run_id"],
                "dag_id": result["dag_id"],
                "file_name": source_file,
                "row_key": str(row.get(key_columns[table], "")),
                "failure_reason": str(row.get("failure_reason", "")),
                "source_record": json.dumps(
                    {key: value for key, value in row.items() if key != "failure_reason"},
                    ensure_ascii=True,
                ),
            })
    for item in result["increment_paths"]:
        failed = _read_passed(item["failed"])
        for row in failed.to_dict(orient="records"):
            quarantine_records.append({
                "run_id": result["run_id"],
                "dag_id": result["dag_id"],
                "file_name": Path(item["source"]).name,
                "row_key": str(row.get("order_id", "")),
                "failure_reason": str(row.get("failure_reason", "")),
                "source_record": json.dumps(
                    {key: value for key, value in row.items() if key != "failure_reason"},
                    ensure_ascii=True,
                ),
            })
    quarantine_frame = pd.DataFrame(
        quarantine_records,
        columns=[
            "run_id", "dag_id", "file_name", "row_key",
            "failure_reason", "source_record",
        ],
    )

    with warehouse_connection() as connection:
        ensure_audit_tables(connection)
        connection.register("etl_customers_input", customers)
        connection.register("etl_products_input", products)
        connection.register("etl_stores_input", stores)
        connection.register("etl_fact_input", fact)
        connection.register("etl_recon_input", reconciliation)
        connection.register("etl_quarantine_input", quarantine_frame)
        connection.execute("BEGIN TRANSACTION")
        try:
            connection.execute(
                """CREATE OR REPLACE TABLE etl_dim_customer AS
                SELECT customer_id, masked_name AS customer_name, masked_email,
                       masked_phone, loyalty_tier
                FROM etl_customers_input"""
            )
            connection.execute(
                """CREATE OR REPLACE TABLE etl_dim_product AS
                SELECT product_id, product_name, category, sub_category, brand,
                       TRY_CAST(mrp AS DOUBLE) mrp, TRY_CAST(cost_price AS DOUBLE) cost_price,
                       unit, is_perishable, launch_date FROM etl_products_input"""
            )
            connection.execute(
                """CREATE OR REPLACE TABLE etl_dim_store AS
                SELECT store_id, store_name, zone, open_date,
                       capacity_orders_per_day FROM etl_stores_input"""
            )
            connection.execute(
                "CREATE OR REPLACE TABLE etl_fact_orders AS SELECT * FROM etl_fact_input"
            )
            connection.execute(
                "CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM etl_recon_input"
            )
            connection.execute(
                """CREATE TABLE IF NOT EXISTS etl_quarantine (
                    run_id VARCHAR, dag_id VARCHAR, file_name VARCHAR, row_key VARCHAR,
                    failure_reason VARCHAR, source_record JSON, checked_at TIMESTAMP
                )"""
            )
            connection.execute(
                "DELETE FROM etl_quarantine WHERE run_id = ? AND dag_id = ?",
                [result["run_id"], result["dag_id"]],
            )
            connection.execute(
                """INSERT INTO etl_quarantine
                   SELECT run_id, dag_id, file_name, row_key, failure_reason,
                          TRY_CAST(source_record AS JSON), CURRENT_TIMESTAMP
                   FROM etl_quarantine_input"""
            )
            connection.execute(
                """CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
                SELECT order_date, zone, store_id, store_name,
                       ROUND(SUM(order_amount), 2) AS net_revenue
                FROM etl_fact_orders WHERE order_status = 'DELIVERED'
                GROUP BY order_date, zone, store_id, store_name"""
            )
            connection.execute(
                """CREATE OR REPLACE TABLE etl_category_margin AS
                SELECT category, ROUND(SUM(order_amount), 2) AS revenue,
                       ROUND(SUM(order_amount - quantity * cost_price), 2) AS gross_margin
                FROM etl_fact_orders WHERE order_status = 'DELIVERED'
                GROUP BY category"""
            )
            _insert_checks(connection, result["run_id"], result["dag_id"], result["checks"])
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            for name in [
                "etl_customers_input", "etl_products_input", "etl_stores_input",
                "etl_fact_input", "etl_recon_input",
                "etl_quarantine_input",
            ]:
                connection.unregister(name)

    return {
        **result,
        "rows_loaded": len(fact),
        "fact_rows": len(fact),
        "reconciliation_rows": len(reconciliation),
    }


def _reconcile(orders: pd.DataFrame, payments: pd.DataFrame) -> pd.DataFrame:
    if orders.empty:
        return pd.DataFrame(columns=[
            "order_id", "order_amount", "amount_paid", "payment_count", "reason",
        ])
    orders = orders[["order_id", "order_amount", "order_status"]].copy()
    orders["order_amount"] = _number(orders["order_amount"])
    payments = payments.copy()
    payments["amount_paid"] = _number(payments["amount_paid"])
    successful = payments.loc[payments["payment_status"].eq("SUCCESS")]
    totals = successful.groupby("order_id", as_index=False).agg(
        amount_paid=("amount_paid", "sum"),
        payment_count=("payment_id", "count"),
    )
    merged = orders.merge(totals, on="order_id", how="left")
    merged["payment_count"] = merged["payment_count"].fillna(0).astype(int)
    merged["reason"] = ""
    active = merged["order_status"].isin(["DELIVERED", "RETURNED"])
    merged.loc[active & merged["payment_count"].eq(0), "reason"] = "MISSING_SUCCESS_PAYMENT"
    merged.loc[
        active & merged["payment_count"].gt(0)
        & (merged["order_amount"] - merged["amount_paid"]).abs().gt(0.01),
        "reason",
    ] = "AMOUNT_MISMATCH"
    duplicates = merged["payment_count"].gt(1)
    merged.loc[active & duplicates & merged["reason"].eq(""), "reason"] = "MULTIPLE_SUCCESS_PAYMENTS"
    return merged.loc[merged["reason"].ne(""), [
        "order_id", "order_amount", "amount_paid", "payment_count", "reason",
    ]]


def run_elt(
    run_id: str,
    dag_id: str,
    source_paths: dict[str, str],
    started_at: str | None = None,
) -> dict[str, Any]:
    source_map = {**{key: str(Path(source_paths.get(key, DATA_DIR / name)))
                     for key, name in FILES.items()}}
    source_map["increment"] = str(
        Path(source_paths.get("increment", DATA_DIR / INCREMENT_FILE))
    )
    for table, filename in FILES.items():
        if not Path(source_map[table]).is_file():
            raise FileNotFoundError(f"Required source file is missing: {source_map[table]}")
    safe_id = safe_run_id(run_id)
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)
    counts: dict[str, int] = {}

    with warehouse_connection() as connection:
        ensure_audit_tables(connection)
        for table, path in source_map.items():
            if table == "increment" and not Path(path).is_file():
                continue
            raw_table = "raw_orders" if table == "increment" else f"raw_{table}"
            with open(path, "r", newline="", encoding="utf-8-sig") as source_file:
                reader = csv.DictReader(source_file)
                if not reader.fieldnames:
                    raise ValueError(f"CSV has no header: {path}")
                fields = reader.fieldnames
                if any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) for name in fields):
                    raise ValueError(f"Unsafe or invalid column name in {path}: {fields}")
                rows = list(reader)
                if any(None in row for row in rows):
                    raise ValueError(f"CSV row has more values than its header: {path}")
            counts[table] = len(rows)
            column_sql = ", ".join(f'"{name}" VARCHAR' for name in fields)
            connection.execute(
                f'CREATE TABLE IF NOT EXISTS "{raw_table}" '
                f'({column_sql}, _loaded_at TIMESTAMP, _source_file VARCHAR, '
                f'_run_id VARCHAR, _source_row BIGINT)'
            )
            warehouse_columns = [
                row[1] for row in connection.execute(
                    f"PRAGMA table_info('{raw_table}')"
                ).fetchall()
            ]
            if warehouse_columns != fields + [
                "_loaded_at", "_source_file", "_run_id", "_source_row",
            ]:
                raise ValueError(
                    f"Schema change detected for {raw_table}: "
                    f"source={fields}, warehouse={warehouse_columns}"
                )
            existing = {
                row[0] for row in connection.execute(
                    f'SELECT _source_row FROM "{raw_table}" '
                    "WHERE _run_id = ? AND _source_file = ?",
                    [run_id, Path(path).name],
                ).fetchall()
            }
            if rows:
                placeholders = ", ".join("?" for _ in fields + [
                    "_loaded_at", "_source_file", "_run_id", "_source_row",
                ])
                insert_sql = f'INSERT INTO "{raw_table}" VALUES ({placeholders})'
                values = [
                    tuple(row.get(field, "") for field in fields)
                    + (loaded_at, Path(path).name, run_id, index)
                    for index, row in enumerate(rows, start=2)
                    if index not in existing
                ]
                if values:
                    connection.executemany(insert_sql, values)
        run_file = SQL_DIR / "nammamart_staging.sql"
        if not run_file.is_file():
            raise FileNotFoundError(f"ELT staging SQL file is missing: {run_file}")
        _execute_sql_file(connection, run_file, run_id, dag_id)
        checks = _elt_quality_checks(connection, run_id)
        _insert_checks(connection, run_id, dag_id, checks)
        _execute_sql_file(connection, SQL_DIR / "nammamart_marts.sql", run_id, dag_id)

    return {
        "run_id": run_id,
        "dag_id": dag_id,
        "started_at": started_at or datetime.now(timezone.utc).isoformat(),
        "rows_extracted": sum(counts.values()),
        "rows_loaded": _table_count("stg_orders"),
        "rows_quarantined": _table_count("elt_quarantine", filter_run=run_id),
        "raw_counts": counts,
        "fact_rows": _table_count("stg_orders", where="dq_status = 'PASS'"),
    }


def _execute_sql_file(
    connection: duckdb.DuckDBPyConnection, path: Path, run_id: str, dag_id: str
) -> None:
    sql = path.read_text(encoding="utf-8")
    sql = sql.replace("{{RUN_ID}}", run_id.replace("'", "''"))
    sql = sql.replace("{{DAG_ID}}", dag_id.replace("'", "''"))
    for index, statement in enumerate(connection.extract_statements(sql), start=1):
        try:
            connection.execute(statement.query)
        except Exception as exc:
            raise RuntimeError(
                f"Failed SQL statement {index} in {path}: {exc}"
            ) from exc


def _elt_quality_checks(
    connection: duckdb.DuckDBPyConnection, run_id: str
) -> list[dict[str, Any]]:
    baseline_count = connection.execute(
        """SELECT rows_checked FROM dq_results
           WHERE check_name = 'elt.orders.volume_baseline'
           ORDER BY checked_at DESC LIMIT 1"""
    ).fetchone()
    queries = [
        ("elt.completeness", "Completeness", "CRITICAL",
         """SELECT
            (SELECT COUNT(*) FROM stg_orders WHERE failure_reason LIKE '%required%') +
            (SELECT COUNT(*) FROM stg_customers WHERE failure_reason LIKE '%required%') +
            (SELECT COUNT(*) FROM stg_products WHERE failure_reason LIKE '%required%') +
            (SELECT COUNT(*) FROM stg_stores WHERE failure_reason LIKE '%required%') +
            (SELECT COUNT(*) FROM stg_payments WHERE failure_reason LIKE '%required%')"""),
        ("elt.uniqueness", "Uniqueness", "CRITICAL",
         """SELECT COUNT(*) FROM elt_quarantine WHERE failure_reason LIKE '%duplicate%'
            AND _run_id = ?"""),
        ("elt.validity", "Validity", "CRITICAL",
         """SELECT
            (SELECT COUNT(*) FROM stg_orders WHERE failure_reason LIKE '%valid%') +
            (SELECT COUNT(*) FROM stg_customers WHERE failure_reason LIKE '%valid%') +
            (SELECT COUNT(*) FROM stg_products WHERE failure_reason LIKE '%valid%') +
            (SELECT COUNT(*) FROM stg_stores WHERE failure_reason LIKE '%valid%') +
            (SELECT COUNT(*) FROM stg_payments WHERE failure_reason LIKE '%valid%')"""),
        ("elt.accuracy", "Accuracy", "CRITICAL",
         """SELECT
            (SELECT COUNT(*) FROM stg_orders WHERE failure_reason LIKE '%accuracy%') +
            (SELECT COUNT(*) FROM stg_products WHERE failure_reason LIKE '%accuracy%') +
            (SELECT COUNT(*) FROM stg_payments WHERE failure_reason LIKE '%accuracy%')"""),
        ("elt.consistency", "Consistency", "CRITICAL",
         """SELECT
            (SELECT COUNT(*) FROM stg_orders WHERE failure_reason LIKE '%consistency%') +
            (SELECT COUNT(*) FROM stg_payments WHERE failure_reason LIKE '%consistency%')"""),
        ("elt.timeliness", "Timeliness", "WARNING",
         """SELECT CASE WHEN MAX(order_ts) IS NULL OR
                    MAX(order_ts) < CURRENT_TIMESTAMP - INTERVAL '1 day'
                    OR COUNT(*) FILTER (WHERE order_ts > CURRENT_TIMESTAMP) > 0
                    THEN 1 ELSE 0 END FROM stg_orders"""),
        ("elt.orders.volume_baseline", "Completeness", "WARNING",
         f"""SELECT CASE WHEN {int(baseline_count[0]) if baseline_count else 0} > 0
                    AND COUNT(*) < {int(baseline_count[0]) if baseline_count else 0} * 0.1
                    THEN 1 ELSE 0 END FROM stg_orders"""),
        ("elt.orders.price_distribution", "Accuracy", "WARNING",
         """SELECT COUNT(*) FROM (
                SELECT store_id, MEDIAN(unit_price) AS store_median
                FROM stg_orders WHERE dq_status = 'PASS'
                GROUP BY store_id
                HAVING MEDIAN(unit_price) > (SELECT MEDIAN(unit_price) * 10 FROM stg_orders)
                    OR MEDIAN(unit_price) < (SELECT MEDIAN(unit_price) / 10 FROM stg_orders)
            ) drifted_stores"""),
        ("elt.orders.delivery_time_range", "Accuracy", "WARNING",
         """SELECT COUNT(*) FROM stg_orders WHERE order_status = 'DELIVERED'
            AND (delivery_minutes < 8 OR delivery_minutes > 35)"""),
    ]
    checks: list[dict[str, Any]] = []
    total = sum(
        int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        for table in (
            "stg_orders", "stg_customers", "stg_products", "stg_stores", "stg_payments"
        )
    )
    for check_name, dimension, severity, query in queries:
        parameters = [run_id] if check_name == "elt.uniqueness" else []
        failed = int(connection.execute(query, parameters).fetchone()[0])
        checks.append({
            "check_name": check_name,
            "dimension": dimension,
            "severity": severity,
            "rows_checked": total,
            "rows_failed": failed,
            "status": "FAIL" if failed and severity == "CRITICAL"
            else "WARN" if failed else "PASS",
        })
    customer_count = int(connection.execute(
        "SELECT COUNT(*) FROM raw_customers WHERE _run_id = ?", [run_id]
    ).fetchone()[0])
    for check_name, predicate in (
        (
            "elt.customers.email_format",
            "NULLIF(TRIM(email), '') IS NOT NULL AND "
            "NOT REGEXP_MATCHES(TRIM(email), '^[^@[:space:]]+@[^@[:space:]]+\\.[^@[:space:]]+$')",
        ),
        (
            "elt.customers.phone_format",
            "NULLIF(TRIM(phone), '') IS NOT NULL AND "
            "NOT REGEXP_MATCHES(TRIM(phone), '^[0-9]{10}$')",
        ),
        (
            "elt.customers.pincode_format",
            "NULLIF(TRIM(pincode), '') IS NOT NULL AND "
            "NOT REGEXP_MATCHES(TRIM(pincode), '^[0-9]{6}$')",
        ),
    ):
        failed = int(connection.execute(
            f"SELECT COUNT(*) FROM raw_customers WHERE _run_id = ? AND {predicate}",
            [run_id],
        ).fetchone()[0])
        checks.append({
            "check_name": check_name,
            "dimension": "Validity",
            "severity": "WARNING",
            "rows_checked": customer_count,
            "rows_failed": failed,
            "status": "WARN" if failed else "PASS",
        })
    return checks


def _table_count(table: str, where: str | None = None, filter_run: str | None = None) -> int:
    with warehouse_connection() as connection:
        condition = ""
        params: list[Any] = []
        if where:
            condition = f" WHERE {where}"
        if filter_run:
            condition += (" AND " if condition else " WHERE ") + "_run_id = ?"
            params.append(filter_run)
        return int(connection.execute(
            f'SELECT COUNT(*) FROM "{table}"{condition}', params
        ).fetchone()[0])


def write_pipeline_audit(
    run_id: str,
    dag_id: str,
    started_at: str,
    status: str,
    metrics: dict[str, Any] | None = None,
) -> None:
    metrics = metrics or {}
    end_time = datetime.now(timezone.utc).replace(tzinfo=None)
    start_time = pd.to_datetime(started_at, utc=True).tz_convert(None).to_pydatetime()
    with warehouse_connection() as connection:
        ensure_audit_tables(connection)
        connection.execute(
            "DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?",
            [run_id, dag_id],
        )
        connection.execute(
            "INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                run_id, dag_id, start_time, end_time,
                int(metrics.get("rows_extracted", 0)),
                int(metrics.get("rows_loaded", 0)),
                int(metrics.get("rows_quarantined", 0)),
                status,
            ],
        )


def verify_elt_raw_row_counts() -> dict[str, int]:
    with warehouse_connection() as connection:
        return {
            table: int(connection.execute(f'SELECT COUNT(*) FROM "raw_{table}"').fetchone()[0])
            for table in FILES
        }


def run_named_sql(
    query: str, parameters: list[Any] | tuple[Any, ...] = ()
) -> list[tuple[Any, ...]]:
    with warehouse_connection() as connection:
        return connection.execute(query, parameters).fetchall()


def profile_csvs() -> dict[str, Any]:
    report: dict[str, Any] = {}
    frames = {
        table: _read_source(filename) for table, filename in FILES.items()
    }
    frames["orders_increment"] = _read_source(INCREMENT_FILE)
    key_columns = {
        "orders": "order_id", "orders_increment": "order_id",
        "payments": "payment_id", "customers": "customer_id",
        "products": "product_id", "stores": "store_id",
    }
    category_columns = {
        "orders": ["order_status", "payment_mode"],
        "orders_increment": ["order_status", "payment_mode"],
        "payments": ["payment_mode", "payment_status"],
        "customers": ["loyalty_tier"],
        "products": ["category"],
        "stores": ["zone"],
    }
    date_columns = {
        "orders": ["order_ts"],
        "orders_increment": ["order_ts"],
        "payments": ["payment_ts"],
        "customers": ["signup_date"],
        "products": ["launch_date"],
        "stores": ["open_date"],
    }
    for table, filename in FILES.items():
        report[table] = _profile_frame(
            table, frames[table], key_columns[table],
            category_columns[table], date_columns[table],
        )
    report["orders_increment"] = _profile_frame(
        "orders_increment", frames["orders_increment"], key_columns["orders_increment"],
        category_columns["orders_increment"], date_columns["orders_increment"],
    )
    checks: list[dict[str, Any]] = []
    clean_customers, _ = _validation(frames["customers"], "customers", checks)
    clean_products, _ = _validation(frames["products"], "products", checks)
    clean_stores, _ = _validation(frames["stores"], "stores", checks)
    clean_orders, _ = _validation(
        frames["orders"], "orders", checks,
        customers=set(clean_customers["customer_id"]),
        products=set(clean_products["product_id"]),
        stores=set(clean_stores["store_id"]),
    )
    _validation(
        frames["orders_increment"], "orders_increment", checks,
        customers=set(clean_customers["customer_id"]),
        products=set(clean_products["product_id"]),
        stores=set(clean_stores["store_id"]),
    )
    _validation(
        frames["payments"], "payments", checks,
        orders=set(clean_orders["order_id"]),
        order_amounts=dict(zip(
            clean_orders["order_id"],
            pd.to_numeric(clean_orders["order_amount"], errors="coerce"),
        )),
    )
    report["referential_integrity"] = {
        "orders.customer_id": int(
            (~frames["orders"]["customer_id"].isin(set(frames["customers"]["customer_id"]))).sum()
        ),
        "orders.product_id": int(
            (~frames["orders"]["product_id"].isin(set(frames["products"]["product_id"]))).sum()
        ),
        "orders.store_id": int(
            (~frames["orders"]["store_id"].isin(set(frames["stores"]["store_id"]))).sum()
        ),
        "payments.order_id": int(
            (~frames["payments"]["order_id"].isin(set(frames["orders"]["order_id"]))).sum()
        ),
    }
    report["quality_checks"] = checks
    return report


def _profile_frame(
    table: str,
    frame: pd.DataFrame,
    key: str,
    categories: list[str],
    dates: list[str],
) -> dict[str, Any]:
    numeric: dict[str, dict[str, float | None]] = {}
    for column in frame.columns:
        parsed = pd.to_numeric(frame[column], errors="coerce")
        present = frame[column].ne("")
        if present.any() and parsed[present].notna().mean() >= 0.9:
            numeric[column] = {
                "min": float(parsed.min()),
                "max": float(parsed.max()),
            }
    date_ranges: dict[str, dict[str, str | None]] = {}
    for column in dates:
        parsed = pd.to_datetime(frame[column], errors="coerce", utc=True).dropna()
        date_ranges[column] = {
            "min": parsed.min().isoformat() if len(parsed) else None,
            "max": parsed.max().isoformat() if len(parsed) else None,
            "invalid": int(frame[column].ne("").sum() - len(parsed)),
        }
    duplicates = frame[key].ne("") & frame[key].duplicated(keep=False)
    return {
        "rows": len(frame),
        "nulls": {
            column: int(frame[column].eq("").sum())
            for column in frame.columns if frame[column].eq("").any()
        },
        "duplicate_key_rows": int(duplicates.sum()),
        "duplicate_key_examples": frame.loc[duplicates, key].drop_duplicates().head(10).tolist(),
        "numeric_ranges": numeric,
        "date_ranges": date_ranges,
        "categories": {
            column: sorted(frame[column].drop_duplicates().tolist())
            for column in categories
        },
    }
