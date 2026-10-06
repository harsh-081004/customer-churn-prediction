# Customer Churn Prediction

An end-to-end big-data analytics capstone for identifying telecom customers who are likely to churn. The project combines distributed storage, SQL analytics, PySpark feature engineering and machine learning, and workflow orchestration:

```text
1. CSV raw data ───► HDFS (Raw Zone)
                       │
2. Apache Hive ────────► Reads raw data for SQL Analytics
                       │
3. Apache Spark ───────► Reads raw data, runs ML predictions
                       │
4. Apache HBase ───────► Spark writes predictions here for instant real-time lookup

Apache Airflow coordinates the complete workflow.
```

The repository contains both reusable command-line scripts and a Jupyter notebook that demonstrates the same HDFS → Hive → Spark flow interactively.

## Contents

- [Project goals](#project-goals)
- [Technology stack](#technology-stack)
- [Repository layout](#repository-layout)
- [Dataset](#dataset)
- [Data flow and outputs](#data-flow-and-outputs)
- [Prerequisites](#prerequisites)
- [Running the project](#running-the-project)
  - [Run HDFS ingestion](#1-run-hdfs-ingestion)
  - [Create Hive tables and run analytics](#2-create-hive-tables-and-run-analytics)
  - [Run Spark analytics and ML](#3-run-spark-analytics-and-ml)
  - [Run the Airflow pipeline](#4-run-the-airflow-pipeline)
  - [Use the Jupyter notebook](#5-use-the-jupyter-notebook)
- [Airflow DAG](#airflow-dag)
- [Analytics and machine-learning workflow](#analytics-and-machine-learning-workflow)
- [Data dictionary](#data-dictionary)
- [Troubleshooting](#troubleshooting)
- [Limitations and production considerations](#limitations-and-production-considerations)
- [Supporting documentation](#supporting-documentation)

## Project goals

The pipeline is designed to answer questions such as:

1. What is the overall customer churn rate?
2. Which contract types have the highest churn?
3. How do internet service, payment method, tenure, and billing relate to churn?
4. Do security and technical-support bundles correlate with lower churn?
5. Which customer segments should a retention team prioritize?
6. How accurately can distributed ML models classify churn risk?

This is an analytics and decision-support project. A model prediction should be treated as a prioritization signal, not as an automatic customer action.

## Technology stack

| Component | Purpose |
| --- | --- |
| Apache Hadoop HDFS | Distributed storage for raw data, processed data, and results |
| Apache Hive | External/managed tables and SQL-based churn analysis |
| Apache Spark / PySpark | Distributed transformations, EDA, feature engineering, and MLlib models |
| Apache HBase | Real-time NoSQL serving layer for instant customer risk-profile lookups |
| Apache Airflow | Weekly orchestration of the end-to-end pipeline |
| Docker Compose | Local Airflow services and supporting PostgreSQL/Redis services |
| Jupyter / PySpark | Interactive execution through `customer_churn_project.ipynb` |
| Python and Bash | Pipeline, ingestion, and verification scripts |

The supplied `airflow/docker-compose.yaml` is a local Airflow development configuration. It does **not** provision the Hadoop NameNode, DataNode, HiveServer, YARN, Spark, or Jupyter services used by the scripts. Those services must already be running in the same Docker/network environment, or the host/container paths and service names must be adapted.

## Repository layout

```text
customer-churn-prediction/
├── .gitignore                     # Tracks files to exclude from git (data, logs, images)
├── README.md                      # Main project documentation
├── MANUAL_RUN.md                  # Cheat sheet for running the pipeline manually via Terminal
├── customer_churn_project.ipynb   # Interactive Jupyter Notebook for EDA & Prototyping
├── generate_graphs.py             # Script to generate graphs from CSV outputs
├── generate_hive_graphs.py        # Script to generate all 10 Hive analytics graphs
├── generate_dummy_data.py         # Utility to generate test data if HDFS is unavailable
├── airflow/                       # Airflow Orchestration
│   ├── docker-compose.yaml        # Docker setup for Airflow + Volume mounts
│   ├── dags/                      
│   │   └── customer_churn_pipeline.py  # The main DAG orchestrating the pipeline
│   ├── logs/                      # Airflow execution logs
│   ├── config/                    # Airflow configuration files
│   └── plugins/                   # Custom Airflow plugins
├── data/                          # Raw Source Data
│   └── customer_churn_data.csv    # The 1MB Telecom dataset (Ignored by git)
├── hdfs_commands/                 # Hadoop / HDFS Scripts
│   ├── hdfs_ingestion.sh          # Creates HDFS paths and uploads the CSV
│   └── hdfs_verify.sh             # Checks HDFS health and uploaded data
├── hive_queries/                  # Hive SQL Analytics
│   ├── 01_create_tables.hql       # SQL to create DB, external, and managed tables
│   └── 02_analytics_queries.hql   # The 10 SQL analytics queries
├── hbase_commands/                # HBase NoSQL Configuration
│   └── hbase_create_table.txt     # Shell commands to create and query real-time profiles
├── spark_analytics/               # PySpark Machine Learning
│   └── spark_churn_analytics.py   # PySpark script for Data Cleaning, LR, RF, and GBT models
├── result/                        # Pipeline Outputs (Ignored by git)
│   ├── eda_summary.csv            # Summary statistics output
│   ├── model_comparison.csv       # Machine learning model scores
│   ├── high_risk_customers.csv    # List of customers predicted to churn
│   ├── q1_overall_churn.png       # Graph: Q1
│   └── q2_churn_by_contract.png   # ... (plus 8 more graphs)

```

## Dataset

`data/customer_churn_data.csv` is a standardized telecom customer churn dataset with 7,043 records. It contains customer demographics, services, contract information, billing values, and the target column `churn`.

The CSV is expected to have a header and comma-separated values. The ingestion and Hive scripts use the HDFS path:

```text
/user/capstone/customer_churn/raw/customer_churn_data.csv
```

## Data flow and outputs

The pipeline uses these HDFS locations:

```text
/user/capstone/customer_churn/
├── raw/
│   └── customer_churn_data.csv
├── processed/
│   └── customer_churn_cleaned.parquet
├── results/
│   ├── eda_summary/
│   ├── model_comparison/
│   ├── churn_predictions.parquet/
│   └── high_risk_customers/
└── hive_warehouse/
```

Hive creates database `customer_churn_db` with:

- `customer_churn_raw`: an external text table reading the CSV in HDFS.
- `customer_churn_processed`: a managed ORC table with a derived `tenure_group`.

Spark writes output directories rather than single files, because HDFS/ Spark writers create distributed part files.

## Prerequisites

Install or make available:

- Docker Desktop with Docker Compose support
- A running Hadoop environment containing:
  - NameNode and DataNode
  - HiveServer2
  - YARN ResourceManager/NodeManager
  - Spark with `spark-submit`
- A Jupyter/PySpark environment if using the notebook
- At least 4 GB memory available to Docker; 8 GB or more is preferable for the complete stack

The scripts assume these service names and endpoints unless changed:

| Service | Expected value |
| --- | --- |
| HDFS NameNode | `namenode:8020` |
| HiveServer2 | `hive-server:10000` |
| Airflow UI/API server | `localhost:8080` |

Make sure the Hadoop and Hive containers can see the project scripts and data. For Airflow, the DAG uses these container paths:

```text
/opt/airflow/data/customer_churn_data.csv
/opt/airflow/hive_queries/
/opt/airflow/spark_analytics/spark_churn_analytics.py
```

The supplied Compose file mounts `dags`, `logs`, `config`, and `plugins`; it does not mount `data`, `hive_queries`, or `spark_analytics` by default. Either add those mounts to the Compose configuration or copy the files into the corresponding Airflow container paths before triggering the DAG.

## Running the project

Run the following commands from the repository root:

```bash
cd customer-churn-prediction
```

### 1. Run HDFS ingestion

Copy the data and ingestion script to the NameNode container:

```bash
docker cp data/customer_churn_data.csv namenode:/tmp/customer_churn_data.csv
docker cp hdfs_commands/hdfs_ingestion.sh namenode:/tmp/hdfs_ingestion.sh
docker exec namenode bash /tmp/hdfs_ingestion.sh
```

The script creates `raw`, `processed`, `results`, and `hive_warehouse` directories, uploads the CSV, and prints file metadata and sample rows.

To verify the cluster and uploaded data:

```bash
docker cp hdfs_commands/hdfs_verify.sh namenode:/tmp/hdfs_verify.sh
docker exec namenode bash /tmp/hdfs_verify.sh
```

Useful manual checks:

```bash
docker exec namenode hdfs dfs -ls -R /user/capstone/customer_churn/
docker exec namenode hdfs dfs -cat \
  /user/capstone/customer_churn/raw/customer_churn_data.csv | head -5
```

### 2. Create Hive tables and run analytics

Copy the Hive scripts to the HiveServer2 container:

```bash
docker cp hive_queries hive-server:/tmp/hive_queries
```

Run them with Beeline:

```bash
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000
```

Then, at the Beeline prompt:

```sql
!run /tmp/hive_queries/01_create_tables.hql
!run /tmp/hive_queries/02_analytics_queries.hql
```

Alternatively, run a script directly:

```bash
docker exec hive-server beeline \
  -u jdbc:hive2://localhost:10000 \
  -f /tmp/hive_queries/01_create_tables.hql
```

The analytics script covers overall churn, contract, internet service, tenure, charges, payment method, senior-citizen, service-bundle, high-value-customer, and multi-dimensional segment analysis.

### 3. Run Spark analytics and ML

Copy the Spark script to the environment that has Spark and PySpark installed, then submit it to YARN:

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  spark_analytics/spark_churn_analytics.py
```

The script reads:

```text
hdfs://namenode:8020/user/capstone/customer_churn/raw/customer_churn_data.csv
```

If your NameNode hostname, port, or HDFS namespace differs, update the constants near the top of `spark_churn_analytics.py` before submitting the job.

### 4. Run the Airflow pipeline

Start the local Airflow services:

```bash
cd airflow
docker compose up -d
```

The Compose file initializes PostgreSQL, Redis, and Airflow services. Open the Airflow UI at [http://localhost:8080](http://localhost:8080). The default local-development credentials are:

```text
username: airflow
password: airflow
```

Trigger the DAG from the CLI:

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger customer_churn_prediction_pipeline
```

Or enable and trigger `customer_churn_prediction_pipeline` from the Airflow UI.

The DAG runs these tasks in order:

```text
validate_source_data
        ↓
create_hdfs_directories
        ↓
upload_data_to_hdfs
        ↓
create_hive_tables
        ↓
run_hive_analytics
        ↓
run_spark_analytics
        ↓
validate_results
        ↓
generate_pipeline_report
        ↓
pipeline_complete
```

Stop the local Airflow services when finished:

```bash
docker compose down
```

Add `-v` only if you intentionally want to delete the local Airflow PostgreSQL volume and its metadata.

### 5. Use the Jupyter notebook

Open `customer_churn_project.ipynb` in a Jupyter/PySpark environment that can access HDFS, Hive, and Spark. Run the cells from top to bottom. The notebook demonstrates:

1. Spark session initialization with Hive support.
2. HDFS directory creation and data upload.
3. Hive database/table setup.
4. SQL analytics.
5. PySpark cleaning, EDA, feature engineering, and ML.
6. Saving predictions and analytical results to HDFS.

The notebook is intended for an environment where commands such as `hdfs`, `beeline`, and `spark-submit` are available inside the notebook container. Adjust local file paths in the notebook if the dataset is mounted elsewhere.

## Airflow DAG

The DAG is configured with:

- DAG ID: `customer_churn_prediction_pipeline`
- Schedule: `@weekly`
- Catchup: disabled
- Retries: 2
- Retry delay: 2 minutes
- Spark task timeout: 30 minutes
- Start date: October 1, 2026

The DAG's `validate_source_data` task checks that the CSV exists, contains at least 100 data rows, and includes `customer_id`, `gender`, and `churn` columns.

The `validate_results` task currently records a validation report for the lightweight Airflow container rather than executing native HDFS checks there. For a production deployment, replace this mocked validation with checks against the HDFS service or a dedicated Hadoop client container.

## Analytics and machine-learning workflow

### Spark transformations

The Spark job:

1. Reads the raw CSV from HDFS with schema inference.
2. Removes duplicate customer IDs and rows without `customer_id` or `churn`.
3. Casts numeric fields to integer/double types.
4. Adds:
   - `tenure_group`
   - `avg_monthly_spend`
   - `charge_ratio`
   - `has_security_bundle`
   - `has_streaming_bundle`
   - `total_services`
   - `churn_label`
5. Writes cleaned data and EDA summaries to HDFS.

### Models and evaluation

The ML pipeline uses Spark MLlib preprocessing with `StringIndexer`, `OneHotEncoder`, `VectorAssembler`, and `StandardScaler`. It trains:

- Logistic Regression
- Random Forest
- Gradient-Boosted Trees

The data is split into training and test sets with an 80/20 split and seed `42`. The job evaluates AUC-ROC, accuracy, F1 score, precision, and recall where implemented, and writes a model comparison table to HDFS.

The Random Forest predictions are used for the saved prediction and high-risk-customer outputs. Model metrics are data- and run-dependent; the README intentionally does not present hard-coded scores as guarantees.

## Data dictionary

| Column | Type | Description |
| --- | --- | --- |
| `customer_id` | string | Unique customer identifier |
| `gender` | string | Customer gender |
| `senior_citizen` | integer | `1` for senior citizen, otherwise `0` |
| `partner` | string | Whether the customer has a partner |
| `dependents` | string | Whether the customer has dependents |
| `tenure_months` | integer | Number of months as a customer |
| `phone_service` | string | Phone service status |
| `multiple_lines` | string | Multiple-line service status |
| `internet_service` | string | DSL, Fiber optic, or No |
| `online_security` | string | Online security service status |
| `online_backup` | string | Online backup service status |
| `device_protection` | string | Device protection service status |
| `tech_support` | string | Technical support service status |
| `streaming_tv` | string | Streaming TV service status |
| `streaming_movies` | string | Streaming movies service status |
| `contract` | string | Month-to-month, One year, or Two year |
| `paperless_billing` | string | Paperless billing status |
| `payment_method` | string | Payment method |
| `monthly_charges` | double | Monthly charge in USD |
| `total_charges` | double | Total charges over tenure in USD |
| `churn` | string | Target label: Yes or No |

## Troubleshooting

| Problem | What to check |
| --- | --- |
| `docker compose up` fails | Confirm Docker has enough memory/CPU and inspect `docker compose logs airflow-init`. |
| Airflow UI is unavailable | Wait for initialization, then run `docker compose ps` and inspect `docker compose logs airflow-apiserver`. |
| DAG is not visible | Confirm the DAG is mounted under `airflow/dags`, then inspect scheduler and DAG processor logs. |
| Airflow cannot find the CSV | Mount the repository `data/` directory at `/opt/airflow/data` or copy the CSV into the configured path. |
| Airflow cannot find Hive/Spark scripts | Mount `hive_queries/` and `spark_analytics/` at the paths configured in the DAG. |
| HDFS upload fails | Run `hdfs dfsadmin -safemode get`, verify the NameNode is healthy, and check the `namenode` service name. |
| Hive returns zero rows | Confirm the raw CSV exists at the HDFS location used by `01_create_tables.hql` and that the header is skipped correctly. |
| Beeline cannot connect | Confirm HiveServer2 is running and that the JDBC host/port (`hive-server:10000`) is correct from the calling container. |
| Spark cannot read HDFS | Confirm `namenode:8020` resolves from the Spark environment and that the raw file exists. |
| Spark job runs out of memory | Increase YARN/container memory or reduce executor parallelism for local development. |
| Results are not found | Remember that Spark writes directories containing part files, not one local output file. |

## Limitations and production considerations

- The Compose file is explicitly for local development and should not be used as a production deployment without hardening.
- Default Airflow credentials and database passwords are development defaults; use secrets in real deployments.
- The DAG contains fixed container paths and service names that must match the deployment environment.
- The result-validation task is intentionally lightweight and currently reports mocked validation.
- Metrics and risk segments should be monitored for class imbalance, data drift, false positives, and fairness across customer groups.
- The dataset contains customer-related information. Apply appropriate access controls, retention rules, and anonymization before using real customer data.
- The ML output is not a substitute for human review or a tested retention policy.

## Supporting documentation

- [Capstone presentation](docs/Capstone_Project_Presentation.pptx)
- [Capstone report](docs/Capstone_Project_Report.docx)

## License and attribution

This repository is a GUVI/Jain University big-data analytics capstone project. The included dataset and supporting materials should be used according to their original source and redistribution terms.
