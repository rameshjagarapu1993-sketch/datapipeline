CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    order_date,
    zone,
    store_id,
    store_name,
    COUNT(*) FILTER (WHERE order_status = 'DELIVERED') AS delivered_orders,
    ROUND(SUM(order_amount) FILTER (WHERE order_status = 'DELIVERED'), 2) AS net_revenue,
    ROUND(AVG(delivery_minutes) FILTER (WHERE order_status = 'DELIVERED'), 2)
        AS avg_delivery_minutes,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE order_status = 'DELIVERED' AND delivery_minutes <= 15
        ) / NULLIF(COUNT(*) FILTER (WHERE order_status = 'DELIVERED'), 0),
        2
    ) AS delivered_within_15_min_pct
FROM stg_orders_enriched
WHERE dq_status = 'PASS'
GROUP BY order_date, zone, store_id, store_name;

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    category,
    ROUND(SUM(order_amount) FILTER (WHERE order_status = 'DELIVERED'), 2) AS revenue,
    ROUND(
        SUM(order_amount - quantity * cost_price)
            FILTER (WHERE order_status = 'DELIVERED'),
        2
    ) AS gross_margin
FROM stg_orders_enriched
WHERE dq_status = 'PASS'
GROUP BY category;

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    store_id,
    store_name,
    zone,
    COUNT(*) AS delivered_orders,
    ROUND(AVG(delivery_minutes), 2) AS avg_delivery_minutes,
    ROUND(100.0 * COUNT(*) FILTER (WHERE delivery_minutes <= 15)
          / NULLIF(COUNT(*), 0), 2) AS within_15_min_pct
FROM stg_orders_enriched
WHERE dq_status = 'PASS' AND order_status = 'DELIVERED'
GROUP BY store_id, store_name, zone;

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH payments AS (
    SELECT
        order_id,
        SUM(TRY_CAST(amount_paid AS DOUBLE))
            FILTER (WHERE UPPER(TRIM(payment_status)) = 'SUCCESS') AS amount_paid,
        COUNT(*) FILTER (WHERE UPPER(TRIM(payment_status)) = 'SUCCESS')
            AS success_payment_count,
        STRING_AGG(DISTINCT
            CASE LOWER(TRIM(payment_mode))
                WHEN 'upi' THEN 'UPI'
                WHEN 'card' THEN 'Card'
                WHEN 'wallet' THEN 'Wallet'
                WHEN 'cash on delivery' THEN 'Cash on Delivery'
                ELSE TRIM(payment_mode)
            END, ', ')
            FILTER (WHERE UPPER(TRIM(payment_status)) = 'SUCCESS') AS payment_mode
    FROM (
        SELECT * FROM raw_payments
        WHERE _run_id = (
            SELECT _run_id FROM raw_payments ORDER BY _loaded_at DESC LIMIT 1
        )
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY payment_id, _source_file, _source_row
            ORDER BY _loaded_at DESC
        ) = 1
    ) p
    GROUP BY order_id
),
compared AS (
    SELECT
        o.order_id,
        o.order_date,
        o.order_status,
        o.payment_mode AS order_payment_mode,
        COALESCE(p.payment_mode, o.payment_mode) AS payment_mode,
        o.order_amount,
        p.amount_paid,
        COALESCE(p.success_payment_count, 0) AS success_payment_count,
        CASE
            WHEN o.order_status IN ('DELIVERED', 'RETURNED')
                 AND COALESCE(p.success_payment_count, 0) = 0
                THEN 'MISSING_SUCCESS_PAYMENT'
            WHEN o.order_status IN ('DELIVERED', 'RETURNED')
                 AND ABS(o.order_amount - p.amount_paid) > 0.01
                THEN 'AMOUNT_MISMATCH'
            WHEN o.order_status IN ('DELIVERED', 'RETURNED')
                 AND p.success_payment_count > 1
                THEN 'MULTIPLE_SUCCESS_PAYMENTS'
            ELSE NULL
        END AS reason
    FROM stg_orders o
    LEFT JOIN payments p USING (order_id)
    WHERE o.dq_status = 'PASS'
)
SELECT order_id, order_date, payment_mode, order_amount, amount_paid,
       success_payment_count, reason,
       ABS(COALESCE(order_amount, 0) - COALESCE(amount_paid, 0)) AS mismatch_amount
FROM compared
WHERE reason IS NOT NULL;

CREATE OR REPLACE TABLE mart_payment_mode_revenue AS
SELECT
    payment_mode,
    COUNT(*) AS delivered_orders,
    ROUND(SUM(order_amount), 2) AS net_revenue
FROM stg_orders
WHERE dq_status = 'PASS' AND order_status = 'DELIVERED'
GROUP BY payment_mode;

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    customer_id,
    customer_name,
    loyalty_tier,
    ROUND(SUM(order_amount), 2) AS total_spend,
    COUNT(*) AS delivered_orders
FROM stg_orders_enriched
WHERE dq_status = 'PASS' AND order_status = 'DELIVERED'
GROUP BY customer_id, customer_name, loyalty_tier
ORDER BY total_spend DESC
LIMIT 10;
