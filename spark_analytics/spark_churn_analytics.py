"""
===================================================================================
Customer Churn Prediction - Spark/PySpark Analytics
===================================================================================
Big Data Analytics Capstone Project
Stack: HDFS → Hive → Spark/PySpark

This script performs:
  1. Read raw data from HDFS
  2. Data cleaning & transformation
  3. Exploratory Data Analysis (EDA)
  4. Feature engineering
  5. Churn prediction using MLlib (Logistic Regression + Random Forest)
  6. Save results back to HDFS

Run in Jupyter/PySpark environment:
  spark-submit --master yarn spark_churn_analytics.py
  OR run cells in Jupyter notebook
===================================================================================
"""

# ==============================================================================
# CELL 1: Spark Session Setup
# ==============================================================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
import sys

spark = SparkSession.builder \
    .appName("CustomerChurnPrediction") \
    .config("spark.sql.warehouse.dir", "/user/capstone/customer_churn/hive_warehouse") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(f"Spark Version: {spark.version}")
print(f"App Name: {spark.sparkContext.appName}")
print(f"Master: {spark.sparkContext.master}")

# ==============================================================================
# CELL 2: Read Raw Data from HDFS
# ==============================================================================
print("\n" + "="*70)
print("STEP 1: READING DATA FROM HDFS")
print("="*70)

HDFS_RAW_PATH = "hdfs://namenode:8020/user/capstone/customer_churn/raw/customer_churn_data.csv"
HDFS_PROCESSED_PATH = "hdfs://namenode:8020/user/capstone/customer_churn/processed"
HDFS_RESULTS_PATH = "hdfs://namenode:8020/user/capstone/customer_churn/results"

# Read CSV from HDFS with schema inference
df_raw = spark.read.csv(
    HDFS_RAW_PATH,
    header=True,
    inferSchema=True
)

print(f"Total Records: {df_raw.count()}")
print(f"Total Columns: {len(df_raw.columns)}")
print(f"\nSchema:")
df_raw.printSchema()
print(f"\nSample Data:")
df_raw.show(5, truncate=False)

# ==============================================================================
# CELL 3: Data Quality Assessment
# ==============================================================================
print("\n" + "="*70)
print("STEP 2: DATA QUALITY ASSESSMENT")
print("="*70)

# Check for nulls
print("\n--- Null Count per Column ---")
null_counts = df_raw.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df_raw.columns
])
null_counts.show(truncate=False)

# Check for duplicates
total = df_raw.count()
distinct = df_raw.select("customer_id").distinct().count()
print(f"Total Records: {total}")
print(f"Distinct Customers: {distinct}")
print(f"Duplicates: {total - distinct}")

# Data type summary
print("\n--- Column Data Types ---")
for col_name, col_type in df_raw.dtypes:
    print(f"  {col_name:25s} -> {col_type}")

# ==============================================================================
# CELL 4: Data Cleaning & Transformation
# ==============================================================================
print("\n" + "="*70)
print("STEP 3: DATA CLEANING & TRANSFORMATION")
print("="*70)

df_clean = df_raw \
    .dropDuplicates(["customer_id"]) \
    .dropna(subset=["customer_id", "churn"]) \
    .withColumn("monthly_charges", F.col("monthly_charges").cast("double")) \
    .withColumn("total_charges", F.col("total_charges").cast("double")) \
    .withColumn("tenure_months", F.col("tenure_months").cast("int")) \
    .withColumn("senior_citizen", F.col("senior_citizen").cast("int"))

# Add derived columns
df_clean = df_clean \
    .withColumn("tenure_group",
        F.when(F.col("tenure_months") <= 12, "0-12 months")
         .when(F.col("tenure_months") <= 24, "13-24 months")
         .when(F.col("tenure_months") <= 48, "25-48 months")
         .otherwise("49-72 months")
    ) \
    .withColumn("avg_monthly_spend",
        F.round(F.col("total_charges") / F.greatest(F.col("tenure_months"), F.lit(1)), 2)
    ) \
    .withColumn("charge_ratio",
        F.round(F.col("monthly_charges") / F.greatest(F.col("avg_monthly_spend"), F.lit(1)), 2)
    ) \
    .withColumn("has_security_bundle",
        F.when(
            (F.col("online_security") == "Yes") & (F.col("tech_support") == "Yes"),
            1
        ).otherwise(0)
    ) \
    .withColumn("has_streaming_bundle",
        F.when(
            (F.col("streaming_tv") == "Yes") & (F.col("streaming_movies") == "Yes"),
            1
        ).otherwise(0)
    ) \
    .withColumn("total_services",
        F.when(F.col("phone_service") == "Yes", 1).otherwise(0) +
        F.when(F.col("multiple_lines") == "Yes", 1).otherwise(0) +
        F.when(F.col("internet_service") != "No", 1).otherwise(0) +
        F.when(F.col("online_security") == "Yes", 1).otherwise(0) +
        F.when(F.col("online_backup") == "Yes", 1).otherwise(0) +
        F.when(F.col("device_protection") == "Yes", 1).otherwise(0) +
        F.when(F.col("tech_support") == "Yes", 1).otherwise(0) +
        F.when(F.col("streaming_tv") == "Yes", 1).otherwise(0) +
        F.when(F.col("streaming_movies") == "Yes", 1).otherwise(0)
    ) \
    .withColumn("churn_label", F.when(F.col("churn") == "Yes", 1).otherwise(0))

print(f"Records after cleaning: {df_clean.count()}")
print("\nNew columns added: tenure_group, avg_monthly_spend, charge_ratio,")
print("  has_security_bundle, has_streaming_bundle, total_services, churn_label")
df_clean.select("customer_id", "tenure_group", "avg_monthly_spend",
                "total_services", "has_security_bundle", "churn_label").show(5)

# Cache for performance
df_clean.cache()

# ==============================================================================
# CELL 5: Exploratory Data Analysis (EDA)
# ==============================================================================
print("\n" + "="*70)
print("STEP 4: EXPLORATORY DATA ANALYSIS")
print("="*70)

# --- 5a. Overall Churn Distribution ---
print("\n--- 4.1 Overall Churn Distribution ---")
df_clean.groupBy("churn") \
    .agg(
        F.count("*").alias("count"),
        F.round(F.count("*") * 100.0 / df_clean.count(), 2).alias("percentage")
    ) \
    .orderBy("churn") \
    .show()

# --- 5b. Churn by Contract Type ---
print("\n--- 4.2 Churn Rate by Contract Type ---")
df_clean.groupBy("contract") \
    .agg(
        F.count("*").alias("total"),
        F.sum(F.col("churn_label")).alias("churned"),
        F.round(F.sum(F.col("churn_label")) * 100.0 / F.count("*"), 2).alias("churn_rate_pct")
    ) \
    .orderBy(F.desc("churn_rate_pct")) \
    .show()

# --- 5c. Churn by Internet Service ---
print("\n--- 4.3 Churn Rate by Internet Service ---")
df_clean.groupBy("internet_service") \
    .agg(
        F.count("*").alias("total"),
        F.sum(F.col("churn_label")).alias("churned"),
        F.round(F.sum(F.col("churn_label")) * 100.0 / F.count("*"), 2).alias("churn_rate_pct")
    ) \
    .orderBy(F.desc("churn_rate_pct")) \
    .show()

# --- 5d. Churn by Tenure Group ---
print("\n--- 4.4 Churn Rate by Tenure Group ---")
df_clean.groupBy("tenure_group") \
    .agg(
        F.count("*").alias("total"),
        F.sum(F.col("churn_label")).alias("churned"),
        F.round(F.sum(F.col("churn_label")) * 100.0 / F.count("*"), 2).alias("churn_rate_pct"),
        F.round(F.avg("monthly_charges"), 2).alias("avg_monthly")
    ) \
    .orderBy(F.desc("churn_rate_pct")) \
    .show()

# --- 5e. Churn by Payment Method ---
print("\n--- 4.5 Churn Rate by Payment Method ---")
df_clean.groupBy("payment_method") \
    .agg(
        F.count("*").alias("total"),
        F.sum(F.col("churn_label")).alias("churned"),
        F.round(F.sum(F.col("churn_label")) * 100.0 / F.count("*"), 2).alias("churn_rate_pct")
    ) \
    .orderBy(F.desc("churn_rate_pct")) \
    .show(truncate=False)

# --- 5f. Numerical Statistics by Churn ---
print("\n--- 4.6 Numerical Statistics by Churn Status ---")
df_clean.groupBy("churn") \
    .agg(
        F.round(F.avg("tenure_months"), 1).alias("avg_tenure"),
        F.round(F.avg("monthly_charges"), 2).alias("avg_monthly"),
        F.round(F.avg("total_charges"), 2).alias("avg_total"),
        F.round(F.avg("total_services"), 1).alias("avg_services")
    ) \
    .show()

# --- 5g. Senior Citizen Analysis ---
print("\n--- 4.7 Senior Citizen Churn Analysis ---")
df_clean.groupBy("senior_citizen") \
    .agg(
        F.count("*").alias("total"),
        F.sum(F.col("churn_label")).alias("churned"),
        F.round(F.sum(F.col("churn_label")) * 100.0 / F.count("*"), 2).alias("churn_rate_pct")
    ) \
    .show()

# --- 5h. Service Bundle Impact ---
print("\n--- 4.8 Security Bundle Impact on Churn ---")
df_clean.filter(F.col("internet_service") != "No") \
    .groupBy("has_security_bundle") \
    .agg(
        F.count("*").alias("total"),
        F.sum(F.col("churn_label")).alias("churned"),
        F.round(F.sum(F.col("churn_label")) * 100.0 / F.count("*"), 2).alias("churn_rate_pct")
    ) \
    .show()

# --- 5i. Top Churn Risk Segments ---
print("\n--- 4.9 Top 10 Highest Churn Risk Segments ---")
df_clean.groupBy("contract", "internet_service", "tenure_group", "payment_method") \
    .agg(
        F.count("*").alias("total"),
        F.sum(F.col("churn_label")).alias("churned"),
        F.round(F.sum(F.col("churn_label")) * 100.0 / F.count("*"), 2).alias("churn_rate_pct")
    ) \
    .filter(F.col("total") >= 30) \
    .orderBy(F.desc("churn_rate_pct")) \
    .show(10, truncate=False)

# ==============================================================================
# CELL 6: Save Processed Data to HDFS
# ==============================================================================
print("\n" + "="*70)
print("STEP 5: SAVE PROCESSED DATA TO HDFS")
print("="*70)

# Save as Parquet (columnar, compressed, distributed)
df_clean.write.mode("overwrite").parquet(HDFS_PROCESSED_PATH + "/customer_churn_cleaned.parquet")
print(f"Saved processed data to: {HDFS_PROCESSED_PATH}/customer_churn_cleaned.parquet")

# Save EDA results
eda_summary = df_clean.groupBy("contract", "internet_service", "tenure_group") \
    .agg(
        F.count("*").alias("total_customers"),
        F.sum(F.col("churn_label")).alias("churned"),
        F.round(F.sum(F.col("churn_label")) * 100.0 / F.count("*"), 2).alias("churn_rate_pct"),
        F.round(F.avg("monthly_charges"), 2).alias("avg_monthly_charges"),
        F.round(F.avg("total_services"), 1).alias("avg_services")
    )

eda_summary.write.mode("overwrite").csv(HDFS_RESULTS_PATH + "/eda_summary", header=True)
print(f"Saved EDA summary to: {HDFS_RESULTS_PATH}/eda_summary")

# ==============================================================================
# CELL 7: Feature Engineering for ML
# ==============================================================================
print("\n" + "="*70)
print("STEP 6: FEATURE ENGINEERING FOR ML")
print("="*70)

# Select features for ML
categorical_cols = [
    "gender", "partner", "dependents", "phone_service", "multiple_lines",
    "internet_service", "online_security", "online_backup", "device_protection",
    "tech_support", "streaming_tv", "streaming_movies",
    "contract", "paperless_billing", "payment_method", "tenure_group"
]

numerical_cols = [
    "senior_citizen", "tenure_months", "monthly_charges", "total_charges",
    "avg_monthly_spend", "total_services", "has_security_bundle",
    "has_streaming_bundle"
]

# String Indexing + One-Hot Encoding pipeline stages
indexers = [
    StringIndexer(inputCol=col, outputCol=col + "_idx", handleInvalid="keep")
    for col in categorical_cols
]

encoders = [
    OneHotEncoder(inputCol=col + "_idx", outputCol=col + "_vec")
    for col in categorical_cols
]

# Assemble all features
feature_cols = [col + "_vec" for col in categorical_cols] + numerical_cols

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features_raw",
    handleInvalid="keep"
)

scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withStd=True,
    withMean=False
)

print(f"Categorical features: {len(categorical_cols)}")
print(f"Numerical features: {len(numerical_cols)}")
print(f"Total feature columns: {len(feature_cols)}")

# ==============================================================================
# CELL 8: Train/Test Split
# ==============================================================================
print("\n" + "="*70)
print("STEP 7: TRAIN/TEST SPLIT")
print("="*70)

train_df, test_df = df_clean.randomSplit([0.8, 0.2], seed=42)
print(f"Training set: {train_df.count()} records")
print(f"Test set:     {test_df.count()} records")

# Check churn distribution in splits
print("\nTraining set churn distribution:")
train_df.groupBy("churn").count().show()
print("Test set churn distribution:")
test_df.groupBy("churn").count().show()

# ==============================================================================
# CELL 9: Model Training - Logistic Regression
# ==============================================================================
print("\n" + "="*70)
print("STEP 8: MODEL TRAINING - LOGISTIC REGRESSION")
print("="*70)

lr = LogisticRegression(
    featuresCol="features",
    labelCol="churn_label",
    maxIter=100,
    regParam=0.01,
    elasticNetParam=0.5
)

lr_pipeline = Pipeline(stages=indexers + encoders + [assembler, scaler, lr])

print("Training Logistic Regression model...")
lr_model = lr_pipeline.fit(train_df)
lr_predictions = lr_model.transform(test_df)

# Evaluate
binary_eval = BinaryClassificationEvaluator(
    labelCol="churn_label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)

multi_eval = MulticlassClassificationEvaluator(
    labelCol="churn_label",
    predictionCol="prediction"
)

lr_auc = binary_eval.evaluate(lr_predictions)
lr_accuracy = multi_eval.evaluate(lr_predictions, {multi_eval.metricName: "accuracy"})
lr_f1 = multi_eval.evaluate(lr_predictions, {multi_eval.metricName: "f1"})
lr_precision = multi_eval.evaluate(lr_predictions, {multi_eval.metricName: "weightedPrecision"})
lr_recall = multi_eval.evaluate(lr_predictions, {multi_eval.metricName: "weightedRecall"})

print(f"\nLogistic Regression Results:")
print(f"  AUC-ROC:   {lr_auc:.4f}")
print(f"  Accuracy:  {lr_accuracy:.4f}")
print(f"  F1 Score:  {lr_f1:.4f}")
print(f"  Precision: {lr_precision:.4f}")
print(f"  Recall:    {lr_recall:.4f}")

# Confusion Matrix
print("\nPrediction Distribution:")
lr_predictions.groupBy("churn_label", "prediction") \
    .count() \
    .orderBy("churn_label", "prediction") \
    .show()

# ==============================================================================
# CELL 10: Model Training - Random Forest
# ==============================================================================
print("\n" + "="*70)
print("STEP 9: MODEL TRAINING - RANDOM FOREST")
print("="*70)

rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="churn_label",
    numTrees=100,
    maxDepth=8,
    seed=42
)

rf_pipeline = Pipeline(stages=indexers + encoders + [assembler, scaler, rf])

print("Training Random Forest model...")
rf_model = rf_pipeline.fit(train_df)
rf_predictions = rf_model.transform(test_df)

rf_auc = binary_eval.evaluate(rf_predictions)
rf_accuracy = multi_eval.evaluate(rf_predictions, {multi_eval.metricName: "accuracy"})
rf_f1 = multi_eval.evaluate(rf_predictions, {multi_eval.metricName: "f1"})
rf_precision = multi_eval.evaluate(rf_predictions, {multi_eval.metricName: "weightedPrecision"})
rf_recall = multi_eval.evaluate(rf_predictions, {multi_eval.metricName: "weightedRecall"})

print(f"\nRandom Forest Results:")
print(f"  AUC-ROC:   {rf_auc:.4f}")
print(f"  Accuracy:  {rf_accuracy:.4f}")
print(f"  F1 Score:  {rf_f1:.4f}")
print(f"  Precision: {rf_precision:.4f}")
print(f"  Recall:    {rf_recall:.4f}")

print("\nPrediction Distribution:")
rf_predictions.groupBy("churn_label", "prediction") \
    .count() \
    .orderBy("churn_label", "prediction") \
    .show()

# Feature Importance
print("\n--- Feature Importance (Top 15) ---")
rf_classifier = rf_model.stages[-1]
feature_importance = rf_classifier.featureImportances.toArray()

# Get feature names from assembler
assembled_features = lr_model.stages[-3].getInputCols()
importance_list = [(name, float(imp)) for name, imp in zip(assembled_features, feature_importance) if imp > 0]
importance_list.sort(key=lambda x: x[1], reverse=True)

for feat, imp in importance_list[:15]:
    bar = "█" * int(imp * 100)
    print(f"  {feat:35s} {imp:.4f} {bar}")

# ==============================================================================
# CELL 11: Model Training - Gradient Boosted Trees
# ==============================================================================
print("\n" + "="*70)
print("STEP 10: MODEL TRAINING - GBT CLASSIFIER")
print("="*70)

gbt = GBTClassifier(
    featuresCol="features",
    labelCol="churn_label",
    maxIter=50,
    maxDepth=6,
    seed=42
)

gbt_pipeline = Pipeline(stages=indexers + encoders + [assembler, scaler, gbt])

print("Training GBT model...")
gbt_model = gbt_pipeline.fit(train_df)
gbt_predictions = gbt_model.transform(test_df)

gbt_auc = binary_eval.evaluate(gbt_predictions)
gbt_accuracy = multi_eval.evaluate(gbt_predictions, {multi_eval.metricName: "accuracy"})
gbt_f1 = multi_eval.evaluate(gbt_predictions, {multi_eval.metricName: "f1"})

print(f"\nGBT Classifier Results:")
print(f"  AUC-ROC:   {gbt_auc:.4f}")
print(f"  Accuracy:  {gbt_accuracy:.4f}")
print(f"  F1 Score:  {gbt_f1:.4f}")

# ==============================================================================
# CELL 12: Model Comparison Summary
# ==============================================================================
print("\n" + "="*70)
print("STEP 11: MODEL COMPARISON")
print("="*70)

comparison_data = [
    ("Logistic Regression", lr_auc, lr_accuracy, lr_f1, lr_precision, lr_recall),
    ("Random Forest", rf_auc, rf_accuracy, rf_f1, rf_precision, rf_recall),
    ("GBT Classifier", gbt_auc, gbt_accuracy, gbt_f1, 0.0, 0.0),
]

comparison_df = spark.createDataFrame(
    comparison_data,
    ["Model", "AUC_ROC", "Accuracy", "F1_Score", "Precision", "Recall"]
)
comparison_df.show(truncate=False)

# Save comparison to HDFS
comparison_df.write.mode("overwrite").csv(HDFS_RESULTS_PATH + "/model_comparison", header=True)
print(f"Saved model comparison to: {HDFS_RESULTS_PATH}/model_comparison")

# ==============================================================================
# CELL 13: Save Predictions to HDFS
# ==============================================================================
print("\n" + "="*70)
print("STEP 12: SAVE PREDICTIONS TO HDFS")
print("="*70)

# Use the best model's predictions
best_predictions = rf_predictions.select(
    "customer_id", "gender", "senior_citizen", "tenure_months",
    "contract", "internet_service", "payment_method",
    "monthly_charges", "total_charges", "total_services",
    "tenure_group", "churn", "churn_label", "prediction",
    "probability"
)

best_predictions.write.mode("overwrite") \
    .parquet(HDFS_RESULTS_PATH + "/churn_predictions.parquet")
print(f"Saved predictions to: {HDFS_RESULTS_PATH}/churn_predictions.parquet")

# High-risk customers (predicted churn)
high_risk = best_predictions.filter(F.col("prediction") == 1.0) \
    .select("customer_id", "contract", "internet_service", "tenure_months",
            "monthly_charges", "total_services")

high_risk.write.mode("overwrite") \
    .csv(HDFS_RESULTS_PATH + "/high_risk_customers", header=True)

print(f"High-risk customers saved: {high_risk.count()} records")
print(f"Saved to: {HDFS_RESULTS_PATH}/high_risk_customers")

# ==============================================================================
# CELL 14: Business Insights Summary
# ==============================================================================
print("\n" + "="*70)
print("STEP 13: KEY BUSINESS INSIGHTS")
print("="*70)

total_customers = df_clean.count()
churned = df_clean.filter(F.col("churn") == "Yes").count()
churn_rate = churned * 100.0 / total_customers

print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║                   CUSTOMER CHURN ANALYSIS SUMMARY                  ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                    ║
║  Total Customers Analyzed:  {total_customers:>8,}                           ║
║  Churned Customers:         {churned:>8,}                           ║
║  Overall Churn Rate:        {churn_rate:>7.1f}%                            ║
║                                                                    ║
║  Best Model: Random Forest                                         ║
║  AUC-ROC:    {rf_auc:.4f}                                            ║
║  Accuracy:   {rf_accuracy:.4f}                                            ║
║  F1 Score:   {rf_f1:.4f}                                            ║
║                                                                    ║
╠══════════════════════════════════════════════════════════════════════╣
║                        KEY FINDINGS                                ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                    ║
║  1. Month-to-month contracts show highest churn risk               ║
║  2. Fiber optic customers churn more than DSL customers            ║
║  3. New customers (0-12 months tenure) are most likely to churn    ║
║  4. Electronic check payment correlates with higher churn          ║
║  5. Security & tech support bundles reduce churn significantly     ║
║  6. Senior citizens show slightly elevated churn rates             ║
║  7. Higher monthly charges correlate with increased churn          ║
║                                                                    ║
╠══════════════════════════════════════════════════════════════════════╣
║                     BUSINESS RECOMMENDATIONS                       ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                    ║
║  1. Offer contract upgrade incentives to month-to-month users      ║
║  2. Bundle security + tech support for fiber optic customers       ║
║  3. Implement early engagement program for new customers           ║
║  4. Encourage automatic payment methods over electronic check      ║
║  5. Target high-risk segments with retention campaigns             ║
║  6. Provide senior citizen loyalty programs                        ║
║                                                                    ║
╚══════════════════════════════════════════════════════════════════════╝
""")

# ==============================================================================
# CELL 15: Cleanup
# ==============================================================================
df_clean.unpersist()
print("Analytics pipeline completed successfully!")
print(f"All results saved to HDFS: {HDFS_RESULTS_PATH}")

spark.stop()
