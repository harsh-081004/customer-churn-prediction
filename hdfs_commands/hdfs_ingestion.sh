#!/bin/bash
# ==============================================================================
# HDFS Data Ingestion Script - Customer Churn Prediction
# ==============================================================================
# This script uploads the customer churn dataset to HDFS.
# Run this INSIDE the NameNode container.
#
# Usage:
#   1. Copy CSV to NameNode container:
#      docker cp data/customer_churn_data.csv namenode:/tmp/
#
#   2. Exec into NameNode:
#      docker exec -it namenode bash
#
#   3. Run this script:
#      bash /tmp/hdfs_ingestion.sh
# ==============================================================================

set -e

echo "=============================================="
echo " HDFS Ingestion - Customer Churn Prediction"
echo "=============================================="

# --- 1. Create HDFS directory structure ---
echo ""
echo "[Step 1] Creating HDFS directory structure..."

hdfs dfs -mkdir -p /user/capstone/customer_churn/raw
hdfs dfs -mkdir -p /user/capstone/customer_churn/processed
hdfs dfs -mkdir -p /user/capstone/customer_churn/results
hdfs dfs -mkdir -p /user/capstone/customer_churn/hive_warehouse

echo "  Created: /user/capstone/customer_churn/raw"
echo "  Created: /user/capstone/customer_churn/processed"
echo "  Created: /user/capstone/customer_churn/results"
echo "  Created: /user/capstone/customer_churn/hive_warehouse"

# --- 2. Upload raw dataset to HDFS ---
echo ""
echo "[Step 2] Uploading dataset to HDFS..."

hdfs dfs -put -f /tmp/customer_churn_data.csv /user/capstone/customer_churn/raw/

echo "  Uploaded: customer_churn_data.csv -> /user/capstone/customer_churn/raw/"

# --- 3. Verify upload ---
echo ""
echo "[Step 3] Verifying HDFS upload..."

echo "  --- Directory listing ---"
hdfs dfs -ls /user/capstone/customer_churn/raw/

echo ""
echo "  --- File size ---"
hdfs dfs -du -h /user/capstone/customer_churn/raw/customer_churn_data.csv

echo ""
echo "  --- First 5 lines ---"
hdfs dfs -cat /user/capstone/customer_churn/raw/customer_churn_data.csv | head -5

echo ""
echo "  --- Total line count ---"
hdfs dfs -cat /user/capstone/customer_churn/raw/customer_churn_data.csv | wc -l

# --- 4. Check replication and block info ---
echo ""
echo "[Step 4] Checking file metadata..."
hdfs fsck /user/capstone/customer_churn/raw/customer_churn_data.csv -files -blocks

echo ""
echo "=============================================="
echo " HDFS Ingestion Complete!"
echo "=============================================="
echo ""
echo " HDFS Structure:"
echo "   /user/capstone/customer_churn/"
echo "   ├── raw/                  (original CSV)"
echo "   ├── processed/            (cleaned data)"
echo "   ├── results/              (analytics output)"
echo "   └── hive_warehouse/       (Hive tables)"
echo ""
