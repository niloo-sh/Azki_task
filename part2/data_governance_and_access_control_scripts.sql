-- ============================================================================
--  DATA GOVERNANCE AND ACCESS CONTROL
-- ============================================================================

-- 1 Create roles
CREATE ROLE IF NOT EXISTS analyst_role;
CREATE ROLE IF NOT EXISTS data_engineer_role;
CREATE ROLE IF NOT EXISTS finance_role;
CREATE ROLE IF NOT EXISTS product_role;
CREATE ROLE IF NOT EXISTS admin_role;

-- 2 Grant permissions
GRANT SELECT ON denormalized_purchase_events TO analyst_role;

GRANT INSERT, SELECT ON user_events TO data_engineer_role;
GRANT INSERT, SELECT ON third_order TO data_engineer_role;
GRANT INSERT, SELECT ON body_order TO data_engineer_role;
GRANT INSERT, SELECT ON medical_order TO data_engineer_role;
GRANT INSERT, SELECT ON fire_order TO data_engineer_role;
GRANT INSERT, SELECT ON financial_order TO data_engineer_role;

GRANT SELECT ON financial_order TO finance_role;
GRANT SELECT ON denormalized_purchase_events TO finance_role;

GRANT SELECT ON third_order TO product_role;
GRANT SELECT ON body_order TO product_role;
GRANT SELECT ON medical_order TO product_role;
GRANT SELECT ON fire_order TO product_role;
GRANT SELECT ON denormalized_purchase_events TO product_role;

GRANT ALL ON *.* TO admin_role;

-- 3 Create users and assign roles
CREATE USER IF NOT EXISTS analyst_user IDENTIFIED BY 'secure_password_123';
GRANT analyst_role TO analyst_user;

CREATE USER IF NOT EXISTS data_engineer_user IDENTIFIED BY 'secure_password_456';
GRANT data_engineer_role TO data_engineer_user;

CREATE USER IF NOT EXISTS finance_user IDENTIFIED BY 'secure_password_789';
GRANT finance_role TO finance_user;

CREATE USER IF NOT EXISTS product_user IDENTIFIED BY 'secure_password_abc';
GRANT product_role TO product_user;

CREATE USER IF NOT EXISTS admin_user IDENTIFIED BY 'secure_password_admin';
GRANT admin_role TO admin_user;

-- 4 Quota management (resource governance)
CREATE QUOTA IF NOT EXISTS analyst_quota
FOR INTERVAL 1 hour
MAX execution_time = 300,
MAX queries = 1000,
MAX result_rows = 1000000,
MAX read_rows = 100000000
TO analyst_role;

CREATE QUOTA IF NOT EXISTS product_quota
FOR INTERVAL 1 hour
MAX execution_time = 600,
MAX queries = 500,
MAX result_rows = 500000,
MAX read_rows = 50000000
TO product_role;

-- 5 Data masking view
CREATE VIEW IF NOT EXISTS masked_purchase_events AS
SELECT
    event_time,
    user_id % 10000 AS masked_user_id,
    session_id,
    event_type,
    channel,
    CASE
        WHEN premium_amount > 10000000 THEN 'HIGH'
        WHEN premium_amount > 5000000 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS premium_category,
    product_type,
    company_name,
    payment_method,
    payment_status,
    CASE
        WHEN net_amount > 5000000 THEN 'HIGH_VALUE'
        WHEN net_amount > 1000000 THEN 'MEDIUM_VALUE'
        ELSE 'LOW_VALUE'
    END AS amount_category
FROM denormalized_purchase_events;


