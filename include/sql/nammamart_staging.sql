CREATE TABLE IF NOT EXISTS elt_quarantine (
    file_name VARCHAR,
    row_key VARCHAR,
    raw_record JSON,
    failure_reason VARCHAR,
    _run_id VARCHAR,
    _source_file VARCHAR,
    _source_row BIGINT,
    _quarantined_at TIMESTAMP
);

CREATE OR REPLACE TABLE stg_customers AS
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY NULLIF(TRIM(customer_id), '')
            ORDER BY _loaded_at DESC, _source_row ASC
        ) AS _rank
    FROM raw_customers
),
checked AS (
    SELECT *,
        CONCAT_WS('; ',
            CASE WHEN NULLIF(TRIM(customer_id), '') IS NULL THEN 'required customer_id missing' END,
            CASE WHEN NULLIF(TRIM(full_name), '') IS NULL THEN 'required full_name missing' END,
            CASE WHEN NULLIF(TRIM(signup_date), '') IS NULL
                      OR TRY_CAST(signup_date AS DATE) IS NULL THEN 'validity signup_date invalid' END,
            CASE WHEN UPPER(TRIM(loyalty_tier)) NOT IN ('BRONZE','SILVER','GOLD','PLATINUM')
                      THEN 'validity loyalty_tier invalid' END,
            CASE WHEN signup_date <> '' AND TRY_CAST(signup_date AS DATE) > CURRENT_DATE
                      THEN 'timeliness signup_date is future' END
        ) AS failure_reason
    FROM ranked
)
SELECT
    TRIM(customer_id) AS customer_id,
    CONCAT('Customer ', TRIM(customer_id)) AS customer_name,
    CASE WHEN STRPOS(email, '@') > 1
         THEN SUBSTRING(email, 1, 2) || '***@' || SUBSTRING(email, STRPOS(email, '@') + 1)
         ELSE 'masked' END AS masked_email,
    CASE WHEN LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '', 'g')) >= 4
         THEN SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 1, 2)
              || '******' || RIGHT(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 2)
         ELSE 'masked' END AS masked_phone,
    CASE UPPER(TRIM(loyalty_tier))
        WHEN 'BRONZE' THEN 'Bronze' WHEN 'SILVER' THEN 'Silver'
        WHEN 'GOLD' THEN 'Gold' WHEN 'PLATINUM' THEN 'Platinum'
        ELSE TRIM(loyalty_tier) END AS loyalty_tier,
    TRY_CAST(signup_date AS DATE) AS signup_date,
    COALESCE(NULLIF(TRIM(is_active), ''), 'unknown') AS is_active,
    CASE WHEN _rank = 1 AND failure_reason = '' THEN 'PASS' ELSE 'FAIL' END AS dq_status,
    CASE WHEN _rank > 1 THEN 'uniqueness duplicate customer_id' ELSE failure_reason END
        AS failure_reason
FROM checked
WHERE _rank = 1;

INSERT INTO elt_quarantine
SELECT 'customers.csv', customer_id, TO_JSON(raw_customers),
       'uniqueness duplicate customer_id', _run_id, _source_file, _source_row,
       CURRENT_TIMESTAMP
FROM raw_customers
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY NULLIF(TRIM(customer_id), ''), _run_id
    ORDER BY _loaded_at DESC, _source_row ASC
) > 1
AND NOT EXISTS (
    SELECT 1 FROM elt_quarantine q
    WHERE q._run_id = raw_customers._run_id
      AND q._source_file = raw_customers._source_file
      AND q._source_row = raw_customers._source_row
      AND q.failure_reason LIKE '%duplicate%'
);

CREATE OR REPLACE TABLE stg_products AS
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY NULLIF(TRIM(product_id), '')
            ORDER BY _loaded_at DESC, _source_row ASC
        ) AS _rank
    FROM raw_products
),
checked AS (
    SELECT *,
        CONCAT_WS('; ',
            CASE WHEN NULLIF(TRIM(product_id), '') IS NULL THEN 'required product_id missing' END,
            CASE WHEN NULLIF(TRIM(product_name), '') IS NULL THEN 'required product_name missing' END,
            CASE WHEN NULLIF(TRIM(category), '') IS NULL THEN 'required category missing' END,
            CASE WHEN TRY_CAST(mrp AS DOUBLE) IS NULL
                      OR TRY_CAST(cost_price AS DOUBLE) IS NULL THEN 'validity price invalid' END,
            CASE WHEN TRY_CAST(mrp AS DOUBLE) <= 0
                      OR TRY_CAST(cost_price AS DOUBLE) > TRY_CAST(mrp AS DOUBLE)
                      THEN 'accuracy price out of range' END,
            CASE WHEN launch_date <> '' AND TRY_CAST(launch_date AS DATE) IS NULL
                      THEN 'validity launch_date invalid' END,
            CASE WHEN launch_date <> '' AND TRY_CAST(launch_date AS DATE) > CURRENT_DATE
                      THEN 'timeliness launch_date is future' END
        ) AS failure_reason
    FROM ranked
)
SELECT
    TRIM(product_id) AS product_id,
    TRIM(product_name) AS product_name,
    TRIM(category) AS category,
    TRIM(sub_category) AS sub_category,
    TRIM(brand) AS brand,
    TRY_CAST(mrp AS DOUBLE) AS mrp,
    TRY_CAST(cost_price AS DOUBLE) AS cost_price,
    TRIM(unit) AS unit,
    TRIM(is_perishable) AS is_perishable,
    TRY_CAST(launch_date AS DATE) AS launch_date,
    CASE WHEN _rank = 1 AND failure_reason = '' THEN 'PASS' ELSE 'FAIL' END AS dq_status,
    failure_reason
FROM checked
WHERE _rank = 1;

INSERT INTO elt_quarantine
SELECT 'products.csv', product_id, TO_JSON(raw_products),
       'uniqueness duplicate product_id', _run_id, _source_file, _source_row,
       CURRENT_TIMESTAMP
FROM raw_products
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY NULLIF(TRIM(product_id), ''), _run_id
    ORDER BY _loaded_at DESC, _source_row DESC
) > 1
AND NOT EXISTS (
    SELECT 1 FROM elt_quarantine q
    WHERE q._run_id = raw_products._run_id
      AND q._source_file = raw_products._source_file
      AND q._source_row = raw_products._source_row
      AND q.failure_reason LIKE '%duplicate%'
);

CREATE OR REPLACE TABLE stg_stores AS
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY NULLIF(TRIM(store_id), '')
            ORDER BY _loaded_at DESC, _source_row ASC
        ) AS _rank
    FROM raw_stores
),
checked AS (
    SELECT *,
        CONCAT_WS('; ',
            CASE WHEN NULLIF(TRIM(store_id), '') IS NULL THEN 'required store_id missing' END,
            CASE WHEN NULLIF(TRIM(store_name), '') IS NULL THEN 'required store_name missing' END,
            CASE WHEN UPPER(TRIM(zone)) NOT IN ('NORTH','SOUTH','EAST','WEST')
                      THEN 'validity zone invalid' END,
            CASE WHEN TRY_CAST(capacity_orders_per_day AS INTEGER) IS NULL
                      THEN 'validity capacity invalid' END,
            CASE WHEN NULLIF(TRIM(pincode), '') IS NOT NULL
                      AND NOT REGEXP_MATCHES(TRIM(pincode), '^[0-9]{6}$')
                      THEN 'validity pincode invalid' END,
            CASE WHEN open_date <> '' AND TRY_CAST(open_date AS DATE) IS NULL
                      THEN 'validity open_date invalid' END,
            CASE WHEN open_date <> '' AND TRY_CAST(open_date AS DATE) > CURRENT_DATE
                      THEN 'timeliness open_date is future' END
        ) AS failure_reason
    FROM ranked
)
SELECT
    TRIM(store_id) AS store_id,
    TRIM(store_name) AS store_name,
    CASE UPPER(TRIM(zone))
        WHEN 'NORTH' THEN 'North' WHEN 'SOUTH' THEN 'South'
        WHEN 'EAST' THEN 'East' WHEN 'WEST' THEN 'West'
        ELSE TRIM(zone) END AS zone,
    TRIM(area) AS area,
    TRIM(pincode) AS pincode,
    TRY_CAST(open_date AS DATE) AS open_date,
    TRY_CAST(capacity_orders_per_day AS INTEGER) AS capacity_orders_per_day,
    CASE WHEN _rank = 1 AND failure_reason = '' THEN 'PASS' ELSE 'FAIL' END AS dq_status,
    failure_reason
FROM checked
WHERE _rank = 1;

CREATE OR REPLACE TEMP TABLE _orders_ranked AS
SELECT *,
    ROW_NUMBER() OVER (
        PARTITION BY NULLIF(TRIM(order_id), '')
        ORDER BY
            CASE WHEN _source_file = 'orders_2026-10-01_increment.csv' THEN 1 ELSE 0 END DESC,
            _loaded_at DESC,
            _source_row ASC
    ) AS _rank
FROM raw_orders;

CREATE OR REPLACE TEMP TABLE _orders_checked AS
WITH normalised AS (
    SELECT *,
        CASE LOWER(TRIM(payment_mode))
            WHEN 'upi' THEN 'UPI'
            WHEN 'card' THEN 'Card'
            WHEN 'wallet' THEN 'Wallet'
            WHEN 'cash on delivery' THEN 'Cash on Delivery'
            ELSE TRIM(payment_mode)
        END AS payment_mode_clean,
        UPPER(TRIM(order_status)) AS order_status_clean,
        TRY_CAST(order_ts AS TIMESTAMP) AS order_ts_clean,
        TRY_CAST(quantity AS INTEGER) AS quantity_clean,
        TRY_CAST(unit_price AS DOUBLE) AS unit_price_clean,
        TRY_CAST(discount_pct AS DOUBLE) AS discount_pct_clean,
        TRY_CAST(order_amount AS DOUBLE) AS order_amount_clean,
        TRY_CAST(delivery_minutes AS INTEGER) AS delivery_minutes_clean
    FROM _orders_ranked
),
checked AS (
    SELECT o.*,
        CONCAT_WS('; ',
            CASE WHEN NULLIF(TRIM(o.order_id), '') IS NULL THEN 'required order_id missing' END,
            CASE WHEN NULLIF(TRIM(o.customer_id), '') IS NULL THEN 'required customer_id missing' END,
            CASE WHEN NULLIF(TRIM(o.store_id), '') IS NULL THEN 'required store_id missing' END,
            CASE WHEN NULLIF(TRIM(o.product_id), '') IS NULL THEN 'required product_id missing' END,
            CASE WHEN order_ts_clean IS NULL THEN 'validity order_ts invalid' END,
            CASE WHEN order_ts_clean > CURRENT_TIMESTAMP THEN 'timeliness order_ts is future' END,
            CASE WHEN order_status_clean NOT IN ('DELIVERED','CANCELLED','RETURNED')
                      THEN 'validity order_status invalid' END,
            CASE WHEN payment_mode_clean NOT IN ('UPI','Card','Cash on Delivery','Wallet')
                      THEN 'validity payment_mode invalid' END,
            CASE WHEN quantity_clean IS NULL OR unit_price_clean IS NULL
                      OR discount_pct_clean IS NULL OR order_amount_clean IS NULL
                      THEN 'validity numeric value invalid' END,
            CASE WHEN quantity_clean <= 0 OR unit_price_clean <= 0
                      OR discount_pct_clean < 0 OR discount_pct_clean > 100
                      OR order_amount_clean < 0
                      THEN 'accuracy value out of range' END,
            CASE WHEN ABS(ROUND(quantity_clean * unit_price_clean
                      * (1 - discount_pct_clean / 100), 2) - order_amount_clean) > 0.01
                      THEN 'accuracy order_amount formula mismatch' END,
            CASE WHEN order_status_clean = 'DELIVERED' AND delivery_minutes_clean IS NULL
                      THEN 'required delivery_minutes missing' END,
            CASE WHEN delivery_minutes_clean >= 120 OR delivery_minutes_clean < 0
                      THEN 'accuracy delivery_minutes out of range' END,
            CASE WHEN c.customer_id IS NULL THEN 'consistency customer foreign key missing' END,
            CASE WHEN p.product_id IS NULL THEN 'consistency product foreign key missing' END,
            CASE WHEN s.store_id IS NULL THEN 'consistency store foreign key missing' END
        ) AS failure_reason
    FROM normalised o
    LEFT JOIN stg_customers c ON c.customer_id = TRIM(o.customer_id) AND c.dq_status = 'PASS'
    LEFT JOIN stg_products p ON p.product_id = TRIM(o.product_id) AND p.dq_status = 'PASS'
    LEFT JOIN stg_stores s ON s.store_id = TRIM(o.store_id) AND s.dq_status = 'PASS'
),
ranked_check AS (
    SELECT *,
        COUNT(*) OVER (
                PARTITION BY order_id, _source_file, _run_id
        ) AS _same_file_count
    FROM checked
)
SELECT *,
    CASE WHEN failure_reason = '' THEN 'PASS' ELSE 'FAIL' END AS dq_status
FROM ranked_check
WHERE _rank = 1;

INSERT INTO elt_quarantine
SELECT 'orders.csv', order_id, TO_JSON(o), failure_reason,
       _run_id, _source_file, _source_row, CURRENT_TIMESTAMP
FROM _orders_checked o
WHERE failure_reason <> ''
AND NOT EXISTS (
    SELECT 1 FROM elt_quarantine q
    WHERE q._run_id = o._run_id AND q._source_file = o._source_file
      AND q._source_row = o._source_row
);

INSERT INTO elt_quarantine
SELECT 'orders.csv', order_id, TO_JSON(o),
       'uniqueness duplicate order_id within source file',
       _run_id, _source_file, _source_row, CURRENT_TIMESTAMP
FROM _orders_ranked o
QUALIFY COUNT(*) OVER (PARTITION BY order_id, _source_file, _run_id) > 1
    AND ROW_NUMBER() OVER (
        PARTITION BY order_id, _source_file, _run_id ORDER BY _source_row
    ) > 1
AND NOT EXISTS (
    SELECT 1 FROM elt_quarantine q
    WHERE q._run_id = o._run_id AND q._source_file = o._source_file
      AND q._source_row = o._source_row
);

CREATE TABLE IF NOT EXISTS stg_orders AS
SELECT
    order_id, order_ts_clean AS order_ts, CAST(order_ts_clean AS DATE) AS order_date,
    customer_id, store_id, product_id, quantity_clean AS quantity,
    unit_price_clean AS unit_price, discount_pct_clean AS discount_pct,
    order_amount_clean AS order_amount, payment_mode_clean AS payment_mode,
    order_status_clean AS order_status, channel,
    delivery_minutes_clean AS delivery_minutes, dq_status, failure_reason,
    _run_id, _source_file, _source_row
FROM _orders_checked WHERE FALSE;

MERGE INTO stg_orders AS target
USING (
    SELECT
        order_id, order_ts_clean AS order_ts, CAST(order_ts_clean AS DATE) AS order_date,
        customer_id, store_id, product_id, quantity_clean AS quantity,
        unit_price_clean AS unit_price, discount_pct_clean AS discount_pct,
        order_amount_clean AS order_amount, payment_mode_clean AS payment_mode,
        order_status_clean AS order_status, channel,
        delivery_minutes_clean AS delivery_minutes, dq_status, failure_reason,
        _run_id, _source_file, _source_row
    FROM _orders_checked
) AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET
    order_ts = source.order_ts, order_date = source.order_date,
    customer_id = source.customer_id, store_id = source.store_id,
    product_id = source.product_id, quantity = source.quantity,
    unit_price = source.unit_price, discount_pct = source.discount_pct,
    order_amount = source.order_amount, payment_mode = source.payment_mode,
    order_status = source.order_status, channel = source.channel,
    delivery_minutes = source.delivery_minutes, dq_status = source.dq_status,
    failure_reason = source.failure_reason, _run_id = source._run_id,
    _source_file = source._source_file, _source_row = source._source_row
WHEN NOT MATCHED THEN INSERT (
    order_id, order_ts, order_date, customer_id, store_id, product_id, quantity,
    unit_price, discount_pct, order_amount, payment_mode, order_status,
    channel, delivery_minutes, dq_status, failure_reason, _run_id,
    _source_file, _source_row
) VALUES (
    source.order_id, source.order_ts, source.order_date, source.customer_id,
    source.store_id, source.product_id, source.quantity, source.unit_price,
    source.discount_pct, source.order_amount, source.payment_mode,
    source.order_status, source.channel, source.delivery_minutes,
    source.dq_status, source.failure_reason, source._run_id,
    source._source_file, source._source_row
);

CREATE OR REPLACE TABLE stg_payments AS
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY NULLIF(TRIM(payment_id), '')
            ORDER BY _loaded_at DESC, _source_row ASC
        ) AS _rank
    FROM raw_payments
),
normalised AS (
    SELECT *,
        CASE LOWER(TRIM(payment_mode))
            WHEN 'upi' THEN 'UPI'
            WHEN 'card' THEN 'Card'
            WHEN 'wallet' THEN 'Wallet'
            WHEN 'cash on delivery' THEN 'Cash on Delivery'
            ELSE TRIM(payment_mode)
        END AS payment_mode_clean,
        UPPER(TRIM(payment_status)) AS payment_status_clean,
        TRY_CAST(payment_ts AS TIMESTAMP) AS payment_ts_clean,
        TRY_CAST(amount_paid AS DOUBLE) AS amount_paid_clean
    FROM ranked
),
checked AS (
    SELECT p.*,
        CONCAT_WS('; ',
            CASE WHEN NULLIF(TRIM(p.payment_id), '') IS NULL THEN 'required payment_id missing' END,
            CASE WHEN NULLIF(TRIM(p.order_id), '') IS NULL THEN 'required order_id missing' END,
            CASE WHEN payment_ts_clean IS NULL THEN 'validity payment_ts invalid' END,
            CASE WHEN payment_ts_clean > CURRENT_TIMESTAMP THEN 'timeliness payment_ts is future' END,
            CASE WHEN payment_status_clean NOT IN ('SUCCESS','FAILED','REFUNDED')
                      THEN 'validity payment_status invalid' END,
            CASE WHEN payment_mode_clean NOT IN ('UPI','Card','Cash on Delivery','Wallet')
                      OR amount_paid_clean IS NULL THEN 'validity payment data invalid' END,
            CASE WHEN amount_paid_clean < 0 THEN 'accuracy amount_paid negative' END,
            CASE WHEN o.order_id IS NULL THEN 'consistency order foreign key missing' END,
            CASE WHEN payment_status_clean = 'SUCCESS' AND o.order_id IS NOT NULL
                      AND ABS(amount_paid_clean - o.order_amount) > 0.01
                      THEN 'consistency payment amount mismatch' END
        ) AS failure_reason
    FROM normalised p
    LEFT JOIN stg_orders o ON o.order_id = TRIM(p.order_id)
)
SELECT
    TRIM(payment_id) AS payment_id, TRIM(order_id) AS order_id,
    payment_ts_clean AS payment_ts, payment_mode_clean AS payment_mode,
    amount_paid_clean AS amount_paid, payment_status_clean AS payment_status,
    gateway_ref,
    CASE WHEN _rank = 1 AND failure_reason = '' THEN 'PASS' ELSE 'FAIL' END AS dq_status,
    CASE WHEN _rank > 1 THEN 'uniqueness duplicate payment_id' ELSE failure_reason END
        AS failure_reason,
    _run_id, _source_file, _source_row
FROM checked
WHERE _rank = 1;

INSERT INTO elt_quarantine
SELECT 'payments.csv', payment_id, TO_JSON(p),
       'uniqueness duplicate payment_id', _run_id, _source_file, _source_row,
       CURRENT_TIMESTAMP
FROM raw_payments p
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY NULLIF(TRIM(payment_id), ''), _run_id
    ORDER BY _loaded_at DESC, _source_row ASC
) > 1
AND NOT EXISTS (
    SELECT 1 FROM elt_quarantine q
    WHERE q._run_id = p._run_id AND q._source_file = p._source_file
      AND q._source_row = p._source_row
);

CREATE OR REPLACE TABLE stg_orders_enriched AS
SELECT
    o.*,
    s.store_name, s.zone, p.product_name, p.category, p.cost_price,
    c.customer_name, c.masked_email, c.masked_phone, c.loyalty_tier
FROM stg_orders o
JOIN stg_stores s ON s.store_id = o.store_id AND s.dq_status = 'PASS'
JOIN stg_products p ON p.product_id = o.product_id AND p.dq_status = 'PASS'
JOIN stg_customers c ON c.customer_id = o.customer_id AND c.dq_status = 'PASS';

INSERT INTO elt_quarantine
SELECT 'stores.csv', s.store_id, TO_JSON(s), d.failure_reason,
       s._run_id, s._source_file, s._source_row, CURRENT_TIMESTAMP
FROM raw_stores s
JOIN stg_stores d USING (store_id)
WHERE d.dq_status = 'FAIL'
AND NOT EXISTS (
    SELECT 1 FROM elt_quarantine q WHERE q._run_id = s._run_id
      AND q._source_file = s._source_file AND q._source_row = s._source_row
);

INSERT INTO elt_quarantine
SELECT 'products.csv', p.product_id, TO_JSON(p), d.failure_reason,
       p._run_id, p._source_file, p._source_row, CURRENT_TIMESTAMP
FROM raw_products p
JOIN stg_products d USING (product_id)
WHERE d.dq_status = 'FAIL'
AND NOT EXISTS (
    SELECT 1 FROM elt_quarantine q WHERE q._run_id = p._run_id
      AND q._source_file = p._source_file AND q._source_row = p._source_row
);

INSERT INTO elt_quarantine
SELECT 'customers.csv', c.customer_id, TO_JSON(c), d.failure_reason,
       c._run_id, c._source_file, c._source_row, CURRENT_TIMESTAMP
FROM raw_customers c
JOIN stg_customers d USING (customer_id)
WHERE d.dq_status = 'FAIL'
AND NOT EXISTS (
    SELECT 1 FROM elt_quarantine q WHERE q._run_id = c._run_id
      AND q._source_file = c._source_file AND q._source_row = c._source_row
);

INSERT INTO elt_quarantine
SELECT 'payments.csv', p.payment_id, TO_JSON(p), d.failure_reason,
       p._run_id, p._source_file, p._source_row, CURRENT_TIMESTAMP
FROM raw_payments p
JOIN stg_payments d USING (payment_id)
WHERE d.dq_status = 'FAIL'
AND NOT EXISTS (
    SELECT 1 FROM elt_quarantine q WHERE q._run_id = p._run_id
      AND q._source_file = p._source_file AND q._source_row = p._source_row
);
