#!/bin/bash
# ==============================================================================
# HDFS Verification & Operations Script - Customer Churn Prediction
# ==============================================================================
# Run INSIDE the NameNode container to verify HDFS health and data integrity.
# ==============================================================================

set -e

echo "=============================================="
echo " HDFS Verification - Customer Churn Prediction"
echo "=============================================="

# --- 1. Check HDFS health ---
echo ""
echo "[1] HDFS Health Report:"
hdfs dfsadmin -report | head -30

# --- 2. List all project directories ---
echo ""
echo "[2] Project directory structure:"
hdfs dfs -ls -R /user/capstone/customer_churn/

# --- 3. File stats ---
echo ""
echo "[3] File statistics:"
hdfs dfs -count /user/capstone/customer_churn/
hdfs dfs -du -h /user/capstone/customer_churn/

# --- 4. Check permissions ---
echo ""
echo "[4] File permissions:"
hdfs dfs -ls /user/capstone/customer_churn/raw/

# --- 5. Sample data validation ---
echo ""
echo "[5] Sample data (first 3 lines):"
hdfs dfs -cat /user/capstone/customer_churn/raw/customer_churn_data.csv | head -3

# --- 6. NameNode safe mode check ---
echo ""
echo "[6] NameNode safe mode status:"
hdfs dfsadmin -safemode get

echo ""
echo "=============================================="
echo " HDFS Verification Complete"
echo "=============================================="
