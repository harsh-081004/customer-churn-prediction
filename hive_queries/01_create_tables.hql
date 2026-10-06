-- ==============================================================================
-- Hive Setup & Schema - Customer Churn Prediction
-- ==============================================================================
-- Run these commands inside the Hive Server container using Beeline:
--   docker exec -it hive-server bash
--   beeline -u jdbc:hive2://localhost:10000
-- ==============================================================================

-- =====================
-- 1. CREATE DATABASE
-- =====================
CREATE DATABASE IF NOT EXISTS customer_churn_db
COMMENT 'Customer Churn Prediction - Big Data Analytics Capstone'
LOCATION '/user/capstone/customer_churn/hive_warehouse';

USE customer_churn_db;

-- =====================
-- 2. CREATE EXTERNAL TABLE (raw data)
-- =====================
-- External table reads CSV directly from HDFS raw directory.
-- This preserves the original data even if the table is dropped.

DROP TABLE IF EXISTS customer_churn_raw;

CREATE EXTERNAL TABLE customer_churn_raw (
    customer_id         STRING      COMMENT 'Unique customer identifier',
    gender              STRING      COMMENT 'Customer gender: Male/Female',
    senior_citizen      INT         COMMENT '1 = senior citizen, 0 = not',
    partner             STRING      COMMENT 'Has partner: Yes/No',
    dependents          STRING      COMMENT 'Has dependents: Yes/No',
    tenure_months       INT         COMMENT 'Months as customer (1-72)',
    phone_service       STRING      COMMENT 'Has phone service: Yes/No',
    multiple_lines      STRING      COMMENT 'Has multiple lines: Yes/No',
    internet_service    STRING      COMMENT 'Internet type: DSL/Fiber optic/No',
    online_security     STRING      COMMENT 'Has online security service',
    online_backup       STRING      COMMENT 'Has online backup service',
    device_protection   STRING      COMMENT 'Has device protection service',
    tech_support        STRING      COMMENT 'Has tech support service',
    streaming_tv        STRING      COMMENT 'Has streaming TV service',
    streaming_movies    STRING      COMMENT 'Has streaming movies service',
    contract            STRING      COMMENT 'Contract type: Month-to-month/One year/Two year',
    paperless_billing   STRING      COMMENT 'Uses paperless billing: Yes/No',
    payment_method      STRING      COMMENT 'Payment method used',
    monthly_charges     DOUBLE      COMMENT 'Monthly charge amount in USD',
    total_charges       DOUBLE      COMMENT 'Total charges over tenure in USD',
    churn               STRING      COMMENT 'Customer churned: Yes/No'
)
COMMENT 'Raw customer churn data loaded from HDFS CSV'
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/capstone/customer_churn/raw'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Verify table creation
DESCRIBE FORMATTED customer_churn_raw;

-- Quick row count
SELECT COUNT(*) AS total_records FROM customer_churn_raw;

-- =====================
-- 3. CREATE MANAGED TABLE (cleaned/processed)
-- =====================
DROP TABLE IF EXISTS customer_churn_processed;

CREATE TABLE customer_churn_processed (
    customer_id         STRING,
    gender              STRING,
    senior_citizen      INT,
    partner             STRING,
    dependents          STRING,
    tenure_months       INT,
    phone_service       STRING,
    multiple_lines      STRING,
    internet_service    STRING,
    online_security     STRING,
    online_backup       STRING,
    device_protection   STRING,
    tech_support        STRING,
    streaming_tv        STRING,
    streaming_movies    STRING,
    contract            STRING,
    paperless_billing   STRING,
    payment_method      STRING,
    monthly_charges     DOUBLE,
    total_charges       DOUBLE,
    churn               STRING,
    tenure_group        STRING
)
COMMENT 'Cleaned and enriched customer churn data'
STORED AS ORC;

-- Insert with tenure grouping
INSERT OVERWRITE TABLE customer_churn_processed
SELECT
    customer_id, gender, senior_citizen, partner, dependents,
    tenure_months, phone_service, multiple_lines, internet_service, online_security,
    online_backup, device_protection, tech_support, streaming_tv, streaming_movies,
    contract, paperless_billing, payment_method, monthly_charges,
    total_charges, churn,
    CASE
        WHEN tenure_months <= 12 THEN '0-12 months'
        WHEN tenure_months <= 24 THEN '13-24 months'
        WHEN tenure_months <= 48 THEN '25-48 months'
        ELSE '49-72 months'
    END AS tenure_group
FROM customer_churn_raw
WHERE customer_id IS NOT NULL
  AND customer_id != 'customer_id';

SELECT COUNT(*) AS processed_records FROM customer_churn_processed;
