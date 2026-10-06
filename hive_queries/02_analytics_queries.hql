-- ==============================================================================
-- Hive Analytics Queries - Customer Churn Prediction
-- ==============================================================================
-- Run inside Beeline after executing 01_create_tables.hql
--   USE customer_churn_db;
-- ==============================================================================

USE customer_churn_db;

-- =====================
-- QUERY 1: Overall Churn Rate
-- =====================
SELECT
    churn,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM customer_churn_processed
GROUP BY churn
ORDER BY churn;


-- =====================
-- QUERY 2: Churn by Contract Type
-- =====================
SELECT
    contract,
    COUNT(*) AS total_customers,
    SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) AS churned,
    ROUND(SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS churn_rate_pct
FROM customer_churn_processed
GROUP BY contract
ORDER BY churn_rate_pct DESC;


-- =====================
-- QUERY 3: Churn by Internet Service Type
-- =====================
SELECT
    internet_service,
    COUNT(*) AS total_customers,
    SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) AS churned,
    ROUND(SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS churn_rate_pct
FROM customer_churn_processed
GROUP BY internet_service
ORDER BY churn_rate_pct DESC;


-- =====================
-- QUERY 4: Churn by Tenure Group
-- =====================
SELECT
    tenure_group,
    COUNT(*) AS total_customers,
    SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) AS churned,
    ROUND(SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS churn_rate_pct
FROM customer_churn_processed
GROUP BY tenure_group
ORDER BY churn_rate_pct DESC;


-- =====================
-- QUERY 5: Average Monthly Charges by Churn Status
-- =====================
SELECT
    churn,
    ROUND(AVG(monthly_charges), 2) AS avg_monthly_charges,
    ROUND(AVG(total_charges), 2) AS avg_total_charges,
    ROUND(AVG(tenure_months), 1) AS avg_tenure_months
FROM customer_churn_processed
GROUP BY churn;


-- =====================
-- QUERY 6: Churn by Payment Method
-- =====================
SELECT
    payment_method,
    COUNT(*) AS total_customers,
    SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) AS churned,
    ROUND(SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS churn_rate_pct
FROM customer_churn_processed
GROUP BY payment_method
ORDER BY churn_rate_pct DESC;


-- =====================
-- QUERY 7: Senior Citizen Churn Analysis
-- =====================
SELECT
    CASE WHEN senior_citizen = 1 THEN 'Senior' ELSE 'Non-Senior' END AS customer_type,
    COUNT(*) AS total_customers,
    SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) AS churned,
    ROUND(SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS churn_rate_pct
FROM customer_churn_processed
GROUP BY senior_citizen
ORDER BY churn_rate_pct DESC;


-- =====================
-- QUERY 8: Service Bundle Impact on Churn
-- =====================
SELECT
    online_security,
    tech_support,
    COUNT(*) AS total_customers,
    SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) AS churned,
    ROUND(SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS churn_rate_pct
FROM customer_churn_processed
WHERE internet_service != 'No'
GROUP BY online_security, tech_support
ORDER BY churn_rate_pct DESC;


-- =====================
-- QUERY 9: High-Value Customer Churn (Top 20% spenders)
-- =====================
SET hive.strict.checks.cartesian.product=false;
WITH Percentile AS (
    SELECT PERCENTILE_APPROX(monthly_charges, 0.80) as p80
    FROM customer_churn_processed
)
SELECT
    c.churn,
    COUNT(*) AS high_value_customers,
    ROUND(AVG(c.monthly_charges), 2) AS avg_monthly,
    ROUND(AVG(c.total_charges), 2) AS avg_total
FROM customer_churn_processed c
CROSS JOIN Percentile p
WHERE c.monthly_charges >= p.p80
GROUP BY c.churn;


-- =====================
-- QUERY 10: Multi-dimensional Churn Risk Segments
-- =====================
SELECT
    contract,
    internet_service,
    tenure_group,
    COUNT(*) AS total_customers,
    SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) AS churned,
    ROUND(SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS churn_rate_pct
FROM customer_churn_processed
GROUP BY contract, internet_service, tenure_group
HAVING COUNT(*) >= 50
ORDER BY churn_rate_pct DESC
LIMIT 20;
