-- ============================================================================
-- ClickHouse Denormalized Purchase Events Setup
-- Part 2 - Query Performance and Data Governance
-- ============================================================================

-- 1. CREATE SOURCE TABLES
-- ============================================================================

CREATE TABLE IF NOT EXISTS user_events
(
    event_time DateTime,
    user_id UInt64,
    session_id String,
    event_type String,
    channel String,
    premium_amount UInt64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, user_id, session_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS third_order
(
    order_id UInt64,
    user_id UInt64,
    order_time DateTime,
    product_name String,
    company_name String,
    price Decimal(18, 2),
    discount_percentage UInt32,
    discount_amount Decimal(18, 2),
    status String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(order_time)
ORDER BY (order_id, user_id, order_time)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS body_order
(
    order_id UInt64,
    user_id UInt64,
    order_time DateTime,
    product_name String,
    company_name String,
    price Decimal(18, 2),
    discount_percentage UInt32,
    discount_amount Decimal(18, 2),
    status String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(order_time)
ORDER BY (order_id, user_id, order_time)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS medical_order
(
    order_id UInt64,
    user_id UInt64,
    order_time DateTime,
    product_name String,
    company_name String,
    price Decimal(18, 2),
    discount_percentage UInt32,
    discount_amount Decimal(18, 2),
    status String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(order_time)
ORDER BY (order_id, user_id, order_time)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS fire_order
(
    order_id UInt64,
    user_id UInt64,
    order_time DateTime,
    product_name String,
    company_name String,
    price Decimal(18, 2),
    discount_percentage UInt32,
    discount_amount Decimal(18, 2),
    status String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(order_time)
ORDER BY (order_id, user_id, order_time)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS financial_order
(
    order_id UInt64,
    user_id UInt64,
    order_time DateTime,
    payment_method String,
    payment_status String,
    total_amount Decimal(18, 2),
    tax_amount Decimal(18, 2),
    discount_amount Decimal(18, 2),
    net_amount Decimal(18, 2),
    currency String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(order_time)
ORDER BY (order_id, user_id, order_time)
SETTINGS index_granularity = 8192;

-- ============================================================================
-- 2. CREATE DENORMALIZED TARGET TABLE (Optimized)
-- ============================================================================

CREATE TABLE IF NOT EXISTS denormalized_purchase_events
(
    -- Event fields
    event_time DateTime,
    user_id UInt64,
    session_id String,
    event_type String,
    channel String,
    premium_amount UInt64,

    -- Order identification
    order_id UInt64,
    order_time DateTime,

    -- Product details (from 4 product tables)
    product_type LowCardinality(String),  -- 'third', 'body', 'medical', 'fire'
    product_name String,
    company_name String,
    product_price Decimal(18, 2),
    product_discount_percentage UInt32,
    product_discount_amount Decimal(18, 2),
    product_status String,

    -- Financial details (from financial_order)
    payment_method String,
    payment_status String,
    total_amount Decimal(18, 2),
    tax_amount Decimal(18, 2),
    discount_amount Decimal(18, 2),
    net_amount Decimal(18, 2),
    currency String,

    -- Metadata for governance
    created_at DateTime DEFAULT now(),
    source_table String
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (user_id, session_id, order_id, product_type, event_time)
SETTINGS index_granularity = 8192;

-- ============================================================================
-- 3. MATERIALIZED VIEWS FOR EACH PRODUCT TYPE
-- ============================================================================

-- Materialized view for Third product orders
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_third_orders_to_denormalized
TO denormalized_purchase_events
AS
SELECT
    ue.event_time,
    ue.user_id,
    ue.session_id,
    ue.event_type,
    ue.channel,
    ue.premium_amount,
    to.order_id,
    to.order_time,
    'third' AS product_type,
    to.product_name,
    to.company_name,
    to.price AS product_price,
    to.discount_percentage AS product_discount_percentage,
    to.discount_amount AS product_discount_amount,
    to.status AS product_status,
    fo.payment_method,
    fo.payment_status,
    fo.total_amount,
    fo.tax_amount,
    fo.discount_amount,
    fo.net_amount,
    fo.currency,
    now() AS created_at,
    'third_order' AS source_table
FROM user_events AS ue
INNER JOIN third_order AS to
    ON ue.user_id = to.user_id
    AND toDate(ue.event_time) = toDate(to.order_time)
    AND ue.event_type = 'purchase'
INNER JOIN financial_order AS fo
    ON to.order_id = fo.order_id
    AND to.user_id = fo.user_id;

-- Materialized view for Body product orders
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_body_orders_to_denormalized
TO denormalized_purchase_events
AS
SELECT
    ue.event_time,
    ue.user_id,
    ue.session_id,
    ue.event_type,
    ue.channel,
    ue.premium_amount,
    bo.order_id,
    bo.order_time,
    'body' AS product_type,
    bo.product_name,
    bo.company_name,
    bo.price AS product_price,
    bo.discount_percentage AS product_discount_percentage,
    bo.discount_amount AS product_discount_amount,
    bo.status AS product_status,
    fo.payment_method,
    fo.payment_status,
    fo.total_amount,
    fo.tax_amount,
    fo.discount_amount,
    fo.net_amount,
    fo.currency,
    now() AS created_at,
    'body_order' AS source_table
FROM user_events AS ue
INNER JOIN body_order AS bo
    ON ue.user_id = bo.user_id
    AND toDate(ue.event_time) = toDate(bo.order_time)
    AND ue.event_type = 'purchase'
INNER JOIN financial_order AS fo
    ON bo.order_id = fo.order_id
    AND bo.user_id = fo.user_id;

-- Materialized view for Medical product orders
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_medical_orders_to_denormalized
TO denormalized_purchase_events
AS
SELECT
    ue.event_time,
    ue.user_id,
    ue.session_id,
    ue.event_type,
    ue.channel,
    ue.premium_amount,
    mo.order_id,
    mo.order_time,
    'medical' AS product_type,
    mo.product_name,
    mo.company_name,
    mo.price AS product_price,
    mo.discount_percentage AS product_discount_percentage,
    mo.discount_amount AS product_discount_amount,
    mo.status AS product_status,
    fo.payment_method,
    fo.payment_status,
    fo.total_amount,
    fo.tax_amount,
    fo.discount_amount,
    fo.net_amount,
    fo.currency,
    now() AS created_at,
    'medical_order' AS source_table
FROM user_events AS ue
INNER JOIN medical_order AS mo
    ON ue.user_id = mo.user_id
    AND toDate(ue.event_time) = toDate(mo.order_time)
    AND ue.event_type = 'purchase'
INNER JOIN financial_order AS fo
    ON mo.order_id = fo.order_id
    AND mo.user_id = fo.user_id;

-- Materialized view for Fire product orders
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_fire_orders_to_denormalized
TO denormalized_purchase_events
AS
SELECT
    ue.event_time,
    ue.user_id,
    ue.session_id,
    ue.event_type,
    ue.channel,
    ue.premium_amount,
    fo_order.order_id,
    fo_order.order_time,
    'fire' AS product_type,
    fo_order.product_name,
    fo_order.company_name,
    fo_order.price AS product_price,
    fo_order.discount_percentage AS product_discount_percentage,
    fo_order.discount_amount AS product_discount_amount,
    fo_order.status AS product_status,
    fo.payment_method,
    fo.payment_status,
    fo.total_amount,
    fo.tax_amount,
    fo.discount_amount,
    fo.net_amount,
    fo.currency,
    now() AS created_at,
    'fire_order' AS source_table
FROM user_events AS ue
INNER JOIN fire_order AS fo_order
    ON ue.user_id = fo_order.user_id
    AND toDate(ue.event_time) = toDate(fo_order.order_time)
    AND ue.event_type = 'purchase'
INNER JOIN financial_order AS fo
    ON fo_order.order_id = fo.order_id
    AND fo_order.user_id = fo.user_id;



