# NammaMart ETL & ELT Submission

**Audience:** Sameer, CTO, NammaMart Quick Commerce  
**Status:** Pipelines and automated scenarios implemented; local Airflow runs verified. Manual UI screenshots and several UI-only experiments remain outstanding (listed in Part F).

## Implementation and verification

- DAGs: [`nammamart_etl.py`](../dags/nammamart_etl.py) and [`nammamart_elt.py`](../dags/nammamart_elt.py).
- Shared Python validation/loading and audit code: [`nammamart_pipeline.py`](../include/nammamart_pipeline.py).
- ELT SQL: [`nammamart_staging.sql`](../include/sql/nammamart_staging.sql) and [`nammamart_marts.sql`](../include/sql/nammamart_marts.sql).
- Warehouse: `include/warehouse_nammamart.duckdb`; the trainer's warehouse is not used.
- The local DuckDB file contains ELT raw customer data, and per-run work/quarantine files may contain source PII. These runtime artifacts are specifically ignored by Git; do not upload or share them. Only the provided source data belongs under `include/data/nammamart/`.
- The six original CSVs are in `include/data/nammamart/`. They were copied as supplied, not manually edited.
- The focused tests passed: **7 passed**. Both DAGs completed successfully in Airflow; the final ELT run passed after the reconciliation fix. `astro dev run dags list-import-errors` returned **No data found**.
- Latest audited run counts: ETL extracted 5,372 source rows, loaded 2,146 fact rows and quarantined 817 rows; ELT extracted 5,372 rows, staged 2,550 distinct orders and quarantined 475 rows. Quarantine totals include the source files/dimensions, not only order facts.
- September delivered net revenue agrees: **₹1,051,936.00** in both pipelines. The reconciliation tables each contain **273** mismatches; ELT's total absolute mismatch amount is **₹145,018.05**.

## Part 0 — Discovery and data profiling

`profile_csvs()` profiled all six inputs before pipeline implementation. Numeric ranges below are observed source ranges (not accepted-business ranges); date values are parsed UTC dates/timestamps. Distinct category values are shown where requested.

| File | Rows; nulls; duplicate keys | Numeric min–max | Date min–max; invalid | Requested category values / notable issue |
|---|---|---|---|---|
| `orders.csv` | 2,412; `order_ts` 1, `customer_id` 18, `delivery_minutes` 164; 12 duplicated keys (24 rows) | quantity −3–5; unit price 10–883; discount 0–250%; amount −1,593–9,576; delivery 8–1,440 min | `order_ts` 2026-09-01–2027-01-01; 5 invalid | status: CANCELLED, DELIVERED, `Pending??`, RETURNED, `delivered`; mode: CARD/Card, Cash on Delivery, `Cashh`, UPI/`UPI `/`upi`, Wallet, `cod` |
| `payments.csv` | 2,228; `gateway_ref` 301; 4 duplicated keys (8 rows) | amount paid −1,593–9,576 | `payment_ts` 2026-09-01–2027-01-01; 0 invalid | modes as above; statuses FAILED, REFUNDED, SUCCESS; 8 raw orphan order keys |
| `customers.csv` | 405; `city` 6; 5 duplicated keys (10 rows) | phone 9,804,341–9,999,393,305; pincode 0–560,102 | signup 2024-06-01–2027-01-05; 0 invalid | loyalty tier BRONZE/Bronze/bronze, Gold, PLATINUM/Platinum, SILVER/Silver; 12 invalid emails, 7 invalid phones, 3 invalid pincodes, 3 future signup dates |
| `products.csv` | 152; `category` 3; 2 duplicated keys (4 rows) | MRP −203–883; cost price 6.79–631.79 | launch 2023-01-03–2025-09-20; 0 invalid | blank plus Dairy & Bakery, Fruits & Vegetables, Household, Personal Care, Snacks & Beverages, Staples; 5 cost prices exceed MRP |
| `stores.csv` | 15; `zone` 1; no duplicate keys | pincode 560,003–560,102; capacity 0–600 | open date 2024-02-14–2025-07-28; 0 invalid | blank plus East, North, South, West; one missing/invalid zone |
| `orders_2026-10-01_increment.csv` | 160; no nulls/duplicate keys | quantity 1–4; unit price 10–883; discount 0–20%; amount 11–2,052; delivery 8–34 min | order timestamp 2026-09-01–2026-10-01; 0 invalid | statuses DELIVERED/RETURNED; all four allowed payment modes; contains 10 status corrections and 150 new order IDs |

The full profile and row-level critical failures can be queried from the per-run quarantine tables/files. Sample Data Issues Log entries:

| File | Row key example(s) | Column / problem | Quality dimension | Proposed rule | Severity |
|---|---|---|---|---|---|
| `orders.csv` | `ORD001462`, `ORD001326` | `order_id` duplicated; 12 repeated IDs | Uniqueness | Keep one canonical row per `order_id`; quarantine duplicate occurrences | Critical |
| `orders.csv` | `ORD000580` | `customer_id` missing | Completeness | Require a customer key and a valid customer master reference | Critical |
| `orders.csv` | `ORD000030` | `store_id` not in valid store master | Consistency | Quarantine unknown store references | Critical |
| `orders.csv` | `ORD001755` | `order_amount` does not match quantity × price × discount | Accuracy | Recompute and compare to cents | Critical |
| `orders.csv` | `ORD000455` | delivery time ≥120 minutes | Accuracy | Reject implausible delivery values; warn outside the normal 8–35 minute band | Critical / Warning |
| `payments.csv` | `PAY0001651`, `PAY0000755` | `payment_id` duplicated; 4 repeated IDs | Uniqueness | One payment per ID; compare successful payment to order amount | Critical |
| `payments.csv` | — (301 rows) | `gateway_ref` absent | Completeness | Require gateway reference for traceability; currently a warning | Warning |
| `customers.csv` | `C00036`, `C00226` | `customer_id` duplicated; invalid or future-dated CRM rows also found | Uniqueness / Timeliness | One canonical customer per ID; reject future signup dates | Critical |
| `customers.csv` | — (12 / 7 / 3 rows) | malformed email / phone / pincode | Validity | Validate email, 10-digit phone and six-digit pincode; retain but warn | Warning |
| `products.csv` | `P0030`, `P0140` | `product_id` duplicated; missing category (3) and cost above MRP (5) | Uniqueness / Completeness / Accuracy | Unique ID, required category, positive prices and cost ≤ MRP | Critical |
| `stores.csv` | — (1 row) | missing/invalid `zone` | Validity | Require North/South/East/West | Critical |
| Increment | 10 overlapping IDs, e.g. `ORD000017` | status changed DELIVERED → RETURNED; 150 additional IDs | Timeliness / Change history | Upsert by `order_id`; keep raw input and update current state | Critical correction, not an error |

Profiled raw referential-integrity exceptions: orders to customers **18**, products **9**, stores **5**; payments to orders **8**. The ETL checks additionally reject references to dimension rows that themselves fail quality gates. Exact row-level rejected records and failure reasons are in `etl_quarantine` / `elt_quarantine` and the generated quarantine CSVs.

**Questions for the client before production:** (1) Are negative quantities ever legitimate returns, or always source errors? (2) Should finance restate a closed September period when an October return arrives, or report both original and restated figures? (3) Who is authorized to approve the 120-minute delivery cutoff and changes to payment reconciliation tolerances?

## Parts A–D — Pipelines, schedule and data quality

### ETL

Six source extract tasks run in parallel (the five base exports and optional increment). Validation separates passed and failed rows; failed records are written with `failure_reason` to per-run quarantine CSVs and `etl_quarantine`. Clean dimensions and facts are joined, amounts and margin derived, customer name replaced with a customer key label, and email/phone masked. Transactional table replacement makes a repeat run idempotent. Tables include `etl_fact_orders`, `etl_dim_customer`, `etl_dim_product`, `etl_dim_store`, `etl_payment_reconciliation`, `etl_daily_zone_revenue` and `etl_category_margin`.

### ELT

Raw tables retain all input columns as text and append `_loaded_at`, `_source_file`, `_run_id` and `_source_row`; re-runs append a new auditable copy by design. SQL files cast, standardize, deduplicate, mask, validate and quarantine records into staging, then build marts. Orders are merged on `order_id`; the increment's 10 corrections overwrite their base versions and 150 new IDs are inserted. The currently staged corrected order `ORD000017` is RETURNED and points to `orders_2026-10-01_increment.csv`, source row 2.

### Schedule

- Cron: `0 9 * * 1-6`; explicit timezone `Asia/Kolkata`; `catchup=False`.
- 09:00 IST is **03:30 UTC**. UTC is the Airflow UI's common display timezone, though the DAG timezone controls the cron schedule.
- If the DAG's timezone were UTC, 09:00 would be 14:30 IST, not the requested local time.
- Retries: 2, delay: 2 minutes, execution timeout: 45 minutes, maximum active runs: 1.
- `catchup=False` avoids backfilling every missed local schedule when the laptop/scheduler is offline. A laptop or Docker that is off at 9:00 cannot run the job then; an always-on deployment is required for the SLA.
- The DAG is scheduled **at 9:00**, not guaranteed to finish before 9:00. Latest local runs succeeded, but their duration and the scheduler's next-run display must be checked in the Airflow UI before asserting the client SLA.

### Data quality coverage

Both implementations check all six dimensions. Critical checks reject/quarantine records; warning checks are logged without blocking otherwise valid rows.

| Dimension | Implemented examples |
|---|---|
| Completeness | Required keys/fields, delivered delivery time, product category |
| Uniqueness | Order, payment and master IDs |
| Validity | Timestamp parseability, status/mode/tier/zone allowlists, contact formats |
| Accuracy | Positive quantity/prices, discount 0–100, amount formula, delivery <120, cost ≤ MRP |
| Consistency | Order-to-customer/product/store and payment-to-order references; amount reconciliation |
| Timeliness | Future timestamps/signups; stale latest-order warning |

Each run stores `run_id`, `dag_id`, check name/dimension/severity, row counts, status and check timestamp in `dq_results`. The `pipeline_runs` audit stores run times, extracted/loaded/quarantined counts and status. Note: `dq_results` is an aggregate scorecard; warning failures are not yet stored as separate row-level issue records. Critical failed source rows are retained with keys/reasons in quarantine.

## Business questions — example SQL and observed results

The following queries are against the final tables. Use the `etl_` or `mart_` table named in each query for the corresponding pipeline.

1. **Daily net revenue by zone/store**

   ```sql
   SELECT order_date, zone, store_name, SUM(net_revenue) AS net_revenue
   FROM etl_daily_zone_revenue
   WHERE order_date = DATE '2026-09-01'
   GROUP BY 1, 2, 3
   ORDER BY 1, 2, 3;
   ```

   For that date, zone totals were East **₹9,337.05**, North **₹8,380.15**, South **₹3,615.00**, West **₹5,790.15**. ELT equivalent: `mart_daily_store_kpis`.

2. **Revenue and gross margin by category**

   ```sql
   SELECT category, revenue, gross_margin
   FROM etl_category_margin ORDER BY category;
   ```

   ETL and ELT category totals match. Examples: Staples **₹240,776.85 revenue / ₹61,194.48 margin**; Household **₹194,112.90 / ₹49,547.43**. ELT equivalent: `mart_category_margin`.

3. **Average delivery time and share within 15 minutes per store**

   ```sql
   SELECT store_name, zone, delivered_orders, avg_delivery_minutes, within_15_min_pct
   FROM mart_delivery_sla ORDER BY store_name;
   ```

   Example: NammaMart Whitefield, East — **155** deliveries, **20.60** minute average, **35.48%** within 15 minutes. ETL equivalent is derived from `etl_fact_orders` (`within_15_min`).

4. **Revenue by payment mode and unreconciled amount**

   ```sql
   SELECT payment_mode, delivered_orders, net_revenue
   FROM mart_payment_mode_revenue ORDER BY payment_mode;
   SELECT COUNT(*) AS mismatches, SUM(mismatch_amount) AS absolute_mismatch_amount
   FROM mart_payment_reconciliation;
   ```

   UPI: **1,056 orders / ₹558,712.30**; Card: **400 / ₹216,908.85**; Cash on Delivery: **272 / ₹153,611.85**; Wallet: **220 / ₹122,703.00**. There are **273** mismatch rows totalling **₹145,018.05** absolute difference. ETL equivalent: `etl_payment_reconciliation`; query `ABS(order_amount - COALESCE(amount_paid, 0))`.

5. **Top 10 customers by spend, without raw contact details**

   ```sql
   SELECT customer_id, customer_name, loyalty_tier, total_spend
   FROM mart_top_customers ORDER BY total_spend DESC LIMIT 10;
   ```

   Top spend was customer C00136 (Bronze), **₹8,149.10**. Reporting marts contain no raw email, phone or full name columns. ETL equivalent: aggregate `etl_fact_orders` by customer ID/name/tier.

6. **Rows rejected today by file and reason**

   ```sql
   SELECT file_name, failure_reason, COUNT(*) AS rejected_rows
   FROM elt_quarantine
   WHERE _run_id = (SELECT run_id FROM pipeline_runs
                    WHERE dag_id = 'nammamart_elt'
                    ORDER BY end_time DESC LIMIT 1)
   GROUP BY 1, 2 ORDER BY 1, 2;
   ```

   Latest ELT quarantine: customers **8**, orders **416**, payments **40**, products **10**, stores **1** (**475 total**). ETL query `etl_quarantine` by `run_id`/`dag_id`; latest ETL total was **817**, including the increment file.

## Part E — Governance answers

1. **Ownership:** Operations owns orders/stores; Finance owns payments and reconciliation definitions; CRM owns customer data; Catalog owns products; the data team owns warehouse implementation. Rule changes require the dataset owner plus Finance/Operations approval and a recorded version/effective date.
2. **Dictionary (business columns):**

   | Table | Column groups, type and meaning | Source / allowed values | PII |
   |---|---|---|---|
   | `etl_fact_orders` | `order_id` (text key), `order_ts` (timestamp), `order_date` (date), customer/store/product IDs (text keys), `quantity` (integer), `unit_price`, `discount_pct`, `order_amount`, `cost_price`, `net_revenue`, `gross_margin` (decimal), `order_status`, `payment_mode` (text), `delivery_minutes` (integer), `within_15_min` (boolean), zone/category and labels (text) | Orders plus customer/product/store masters; status DELIVERED/CANCELLED/RETURNED; mode UPI/Card/Cash on Delivery/Wallet | No direct contact data; customer key is linkable |
   | `etl_dim_customer` | `customer_id` (key), `customer_name`, `masked_email`, `masked_phone`, `loyalty_tier` (text) | CRM; tiers Bronze/Silver/Gold/Platinum | Masked email/phone; name replaced by a customer-key label |
   | `etl_dim_product` | `product_id`, name/category/subcategory/brand/unit (text), `mrp`, `cost_price` (decimal), `is_perishable` (boolean/text), `launch_date` (date) | Product catalogue; category from catalogue; prices positive and cost ≤ MRP | No |
   | `etl_dim_store` | `store_id`, name, zone, open date, capacity (key/text/date/integer) | Store master; zone North/South/East/West | Store manager name is excluded from reporting |
   | ELT raw/staging/marts | Raw values remain text plus lineage metadata; staging casts and adds `dq_status`/`failure_reason`; marts expose the business columns above plus payment/SLA/customer aggregates | Source CSVs, then SQL transformations | Raw `raw_customers` contains direct identifiers; staging/marts use masked identifiers |

3. **Lineage:** `etl_fact_orders` and `stg_orders` both identify source file and row. For example, corrected order `ORD000017` has current status RETURNED and lineage to increment file row 2. ELT is easier to audit because it preserves every raw version and run; ETL's current facts are replaced on rerun.
4. **Personal data:** Customer `full_name`, email, phone and location fields (`city`, `area`, `pincode`) are identifying or linkable; store `manager_name` is also staff personal data and is excluded from marts. Reports mask email/phone and replace names with `Customer <id>` rather than deleting the customer key needed for grouped metrics. India's Digital Personal Data Protection Act, 2023 is relevant; NammaMart should establish a lawful purpose, minimize use, secure access, honor applicable data-principal rights, and delete data when its purpose/retention basis ends. This is an engineering summary, not legal advice.
5. **Access control:** Raw tables (especially `raw_customers`) should be restricted to named ingestion/compliance staff; staging to data engineers and approved analysts; marts to business users; quarantine to data quality stewards with audited access. No general user should see unmasked raw CRM contacts.
6. **Retention:** Proposed policy: raw source data 12 months, quarantine 90 days after resolution, masked marts according to finance/legal reporting needs (e.g. 7 years for required accounting records). At expiry, securely delete or irreversibly anonymize; confirm periods with Legal/Finance before production.
7. **Secrets:** No credentials are needed by this local pipeline. Future Slack/database credentials belong in Airflow Connections/Variables or a secrets backend, retrieved at runtime (for example `Variable.get()`), never committed in a DAG or GitHub.
8. **Audit example:** `ORD000030` is retained in `raw_orders`; quarantine records it with `consistency store foreign key missing`. It is absent from `etl_fact_orders` (zero matching rows) and ELT marts filter `dq_status = 'PASS'`, so it cannot contribute to CFO revenue. Query the run's `elt_quarantine` row and join on `order_id` to inspect the raw JSON and failure.
9. **Change history:** Ten September orders changed DELIVERED → RETURNED on the October increment. Their prior delivered amounts total **₹4,193.70**, so the current restated September revenue is ₹1,051,936.00; the as-was total was ₹1,056,129.70. Finance should use the restated view for current net revenue while retaining a period-close snapshot/event history for the originally reported view. ELT raw history makes the change auditable; ETL's replaced fact alone does not preserve the former state.

## Part F — Observability experiments

Automated tests use temporary copies of the supplied files; they do not modify the six source exports. These are code/test observations, not claims that every action was performed in the Airflow UI.

| # | Experiment | Expected → Observed | Evidence / follow-up |
|---|---|---|---|
| 1 | Happy path | Both DAGs succeed → both completed successfully against the supplied data | Airflow DAG test/audit and task logs; capture Graph-view screenshots and task-duration evidence for submission |
| 2 | Re-run | ETL fact/revenue stable; ELT raw append-only, staging/marts stable → automated test confirmed ETL stability and doubled raw rows across two ELT runs while revenue stayed matched | Focused integration test; repeat trigger in UI to demonstrate this live |
| 3 | Increment | 150 new IDs + 10 updates, no duplicate business facts → observed 10 overlapping corrected IDs and 150 new IDs; `ORD000017` is RETURNED | `stg_orders` merge, raw lineage; ETL/ELT totals agree |
| 4 | Missing source | Task retries then fails loudly → validator raises `FileNotFoundError`; Airflow retry-state/timing and alert were not manually exercised | Automated missing-path case; rename/restore a file and capture task state/logs in UI |
| 5 | New bad orders | Five injected defects quarantined with reasons and DQ evidence → synthetic ETL test confirms missing customer, negative quantity, unknown store, invalid status and future timestamp are rejected with reasons | Test uses a temporary copy; run equivalent synthetic ELT end-to-end and inspect its DQ rows in UI before sign-off |
| 6 | Schema change | Renamed `order_amount` fails loudly → ELT test raises `ValueError: Schema change detected` | Automated test; UI task-failure screenshot not captured |
| 7 | Volume anomaly | 50 orders vs 2,412 baseline triggers >90% warning → volume check failed as expected in automated test | `orders.volume_baseline` in `dq_results`; implement alert routing for production |
| 8 | Distribution drift | 100× price shift in one store triggers warning → store-level price distribution check detected the synthetic drift | Automated test; a mart-level delta was not separately measured; record before/after KPI in a UI run |
| 9 | Freshness | 30-day-old latest order warns → stale 50-row fixture triggered the timeliness warning | Automated test / DQ check. Future-dated source values should not be allowed to mask freshness, and were separately rejected |
| 10 | Recovery | Clear reruns failed task and downstream only → not tested in Airflow UI | Demonstrate Clear/recovery manually and record task states before acceptance |

**Observability pillars:** Freshness (stale timestamp check), volume (90% baseline warning), schema (required columns/schema-change error), distribution (per-store unit-price median), lineage (run/file/row metadata and raw append history). Lineage is strongest; distribution and alert delivery are weakest. The local implementation logs checks/audits but does not yet send an alert or preserve row-level warning records.

**Submission evidence still needed:** screenshots of both DAG Graph views, schedule/next-run in UI, a quarantine file, `dq_results`, `pipeline_runs` and payment reconciliation; UI-driven experiments 4, 5 (ELT), 6, 8 metric delta and 10; evidence that scheduled runs finish before the client deadline. The local DAG schedule itself is configured, but a run scheduled at 09:00 cannot be assumed ready before 09:00.

## Part G — Recommendation to Sameer

**Recommendation: use ETL as the finance-serving contract, with a restricted append-only ELT/raw archive as the audit and analytics source.** The measured September delivered revenue matches across both marts at ₹1,051,936.00; both reconciliation tables report the same 273 mismatches after the latest-run payment selection fix. ETL prevents failed critical rows from entering the finance fact table and provides familiar quarantine files. ELT preserves source rows, lineage, and late corrections; the tested increment changes ten September statuses to RETURNED and appends 150 new orders without duplicating order IDs in staging.

The dual-layer design resolves the client's conflict: finance reads only validated marts; compliance and analytics can replay restricted raw history and investigate rejected rows. Changes to rules are easier to prototype in SQL, but must be reviewed/versioned to keep ETL and ELT definitions aligned. At 1,000× volume, pandas plus a local DuckDB file and a single-writer lock will be a capacity/availability risk; the current setup demonstrates logic, not production scale. A larger deployment should retain the contracts and open formats while moving compute/storage to a resilient, appropriately sized OSS warehouse/orchestrated environment. The local stack has low software cost, but production operations, backups, access control, retention and monitoring still have real costs.

Top risks of the recommendation:

1. Raw CRM data creates a high-impact privacy exposure if raw access, encryption, retention and audit are not enforced.
2. ETL and ELT can diverge as rules evolve; shared definitions, versioned rules and reconciliation gates are needed.
3. A laptop/local DuckDB deployment cannot guarantee the 9 AM SLA or 1,000× growth; use always-on infrastructure, backups, alerting and load tests before production.
