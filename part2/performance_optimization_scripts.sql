-- ============================================================================
-- PERFORMANCE OPTIMIZATIONS
-- ============================================================================


-- 1 Projection
-- 1.1 Projection for common queries (channel analysis)
ALTER TABLE denormalized_purchase_events
ADD PROJECTION IF NOT EXISTS projection_channel_analysis
(
    SELECT
        channel,
        product_type,
        toYYYYMM(event_time) AS month,
        count() AS purchase_count,
        sum(net_amount) AS total_revenue
    GROUP BY channel, product_type, month
);

ALTER TABLE denormalized_purchase_events MATERIALIZE PROJECTION projection_channel_analysis

-- 1.2 Projection for user analysis
ALTER TABLE denormalized_purchase_events
ADD PROJECTION IF NOT EXISTS projection_user_analysis
(
    SELECT
        user_id,
        product_type,
        count() AS purchase_count,
        sum(net_amount) AS total_spent
    GROUP BY user_id, product_type
);

ALTER TABLE denormalized_purchase_events MATERIALIZE PROJECTION projection_user_analysis

-- ============================================================================

-- 2 Secondary index for payment status

ALTER TABLE denormalized_purchase_events
ADD INDEX IF NOT EXISTS idx_payment_status payment_status TYPE set(100) GRANULARITY 4;

ALTER TABLE denormalized_purchase_events MATERIALIZE INDEX idx_payment_status;

-- =============================================================================
-- 3 Materialized columns for computed values



ALTER TABLE denormalized_purchase_events
ADD COLUMN IF NOT EXISTS revenue_per_unit Decimal(18, 2) MATERIALIZED
    CASE WHEN product_price > 0 THEN net_amount / product_price ELSE 0 END;


ALTER TABLE denormalized_purchase_events
add COLUMN is_high_value UInt8  DEFAULT CASE WHEN net_amount > 1000000 THEN 1 ELSE 0 END;

ALTER TABLE denormalized_purchase_events
ADD COLUMN IF NOT EXISTS is_high_value UInt8 MATERIALIZED
    CASE WHEN net_amount > 1000000 THEN 1 ELSE 0 END;