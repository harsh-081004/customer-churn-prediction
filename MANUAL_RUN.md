# Manual Pipeline Execution Guide

This guide provides the exact terminal commands to run the entire Customer Churn Big Data Pipeline manually via the command line (Terminal), bypassing Airflow entirely.

Make sure you are in the project root folder before running these commands:
```bash
cd "/Users/harshsmac/Desktop/GUVI project/customer-churn-prediction"
```

## 1. Data Ingestion (HDFS)
*Upload the raw CSV data into Hadoop's Distributed File System.*
```bash
# Create the HDFS directories
docker exec namenode hdfs dfs -mkdir -p /user/capstone/customer_churn/raw
docker exec namenode hdfs dfs -mkdir -p /user/capstone/customer_churn/processed
docker exec namenode hdfs dfs -mkdir -p /user/capstone/customer_churn/results
docker exec namenode hdfs dfs -mkdir -p /user/capstone/customer_churn/hive_warehouse

# Copy the local CSV into the namenode container, then upload it to HDFS
docker cp "data/customer_churn_data.csv" namenode:/tmp/customer_churn_data.csv
docker exec namenode hdfs dfs -put -f /tmp/customer_churn_data.csv /user/capstone/customer_churn/raw/
```

## 2. Table Creation & Analytics (Hive)
*Execute the `.hql` scripts to build Hive tables and run SQL analytics.*
```bash
# Copy the hive scripts into the hive-server container
docker cp "hive_queries" hive-server:/tmp/hive_queries

# Run the table creation script
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /tmp/hive_queries/01_create_tables.hql

# Run the analytics queries
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /tmp/hive_queries/02_analytics_queries.hql
```

## 3. Machine Learning (Spark)
*Submit the PySpark job to clean data, train models (Logistic Regression, Random Forest, GBT), and output results to HDFS.*
```bash
# Copy the Python script into the spark master container
docker cp "spark_analytics/spark_churn_analytics.py" spark-master:/tmp/spark_churn_analytics.py

# Submit the Spark job
docker exec spark-master spark-submit \
  --master local[*] \
  --driver-memory 2g \
  /tmp/spark_churn_analytics.py
```

## 4. Export Results (HDFS to Mac)
*Merge the distributed HDFS files and pull them back to your local `result/` folder.*
```bash
# Merge and pull the HDFS files to the Mac's local result/ folder
docker exec namenode hdfs dfs -getmerge /user/capstone/customer_churn/results/eda_summary result/eda_summary.csv
docker exec namenode hdfs dfs -getmerge /user/capstone/customer_churn/results/model_comparison result/model_comparison.csv
docker exec namenode hdfs dfs -getmerge /user/capstone/customer_churn/results/high_risk_customers result/high_risk_customers.csv

# Clean up any duplicated headers in the CSV files
awk 'NR==1 {h=$0; print} NR>1 && $0!=h {print}' result/eda_summary.csv > result/eda_summary_clean.csv && mv result/eda_summary_clean.csv result/eda_summary.csv
awk 'NR==1 {h=$0; print} NR>1 && $0!=h {print}' result/model_comparison.csv > result/model_comparison_clean.csv && mv result/model_comparison_clean.csv result/model_comparison.csv
awk 'NR==1 {h=$0; print} NR>1 && $0!=h {print}' result/high_risk_customers.csv > result/high_risk_clean.csv && mv result/high_risk_clean.csv result/high_risk_customers.csv
```

## 5. Generate Visualizations (Python)
*Run the local Python scripts to read the exported CSVs and turn them into PNG charts.*
```bash
# Run these directly on your Mac terminal
python3 generate_graphs.py
python3 generate_hive_graphs.py
```

## 6. Real-time Customer Serving (HBase)
*Use the HBase NoSQL database to instantly retrieve a customer's churn risk profile using their customer ID.*
```bash
# Copy the hbase script into the hbase container (assuming container is named 'hbase-master')
docker cp "hbase_commands/hbase_create_table.txt" hbase-master:/tmp/hbase_create_table.txt

# Run the HBase shell to execute the script
docker exec -it hbase-master hbase shell /tmp/hbase_create_table.txt
```
