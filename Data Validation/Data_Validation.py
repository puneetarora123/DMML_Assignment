# Databricks notebook source
# MAGIC %md
# MAGIC Automated Data Validation Script (Pandas)
# MAGIC ------------------------------------------------------
# MAGIC - Reads dataset
# MAGIC - Validates schema, datatypes, ranges, allowed values
# MAGIC - Generates visuals
# MAGIC - Creates a text report with issues and recommendations

# COMMAND ----------

# DBTITLE 1,Install Libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import os

# COMMAND ----------

merged_temp_table = "dmml.raw.raw_customer_churn_temp"

# COMMAND ----------

# DBTITLE 1,Load data for current batch
# Load data
data = spark.table(merged_temp_table).toPandas()

# COMMAND ----------

# DBTITLE 1,Validate count
data.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Checks

# COMMAND ----------

validation_results = []

# COMMAND ----------

# MAGIC %md
# MAGIC #### Missing Columns check 

# COMMAND ----------

# Expected schema
expected_columns = [
    "CustomerID", "Age", "Gender", "Tenure", "MonthlyCharges",
    "TotalCharges", "ContractType", "PaymentMethod"
]

# COMMAND ----------

# Column presence
missing_columns = [col for col in expected_columns if col not in data.columns]
if missing_columns:
    validation_results.append(f" Missing columns: {missing_columns}")
else:
    validation_results.append("All expected columns are present.")
    
print(missing_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Null values check 

# COMMAND ----------

null_counts = data.isnull().sum()
null_issues = null_counts[null_counts > 0]
if not null_issues.empty:
    validation_results.append(f" Null values found:\n{null_issues}")
else:
    validation_results.append(" No null values found.")

print(null_issues)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Duplicates check 

# COMMAND ----------

dup_count = data.duplicated().sum()
if dup_count > 0:
    validation_results.append(f" Found {dup_count} duplicate rows.")
else:
    validation_results.append(" No duplicate rows.")

print(dup_count)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #### Data type checks

# COMMAND ----------

# Ranges for numeric columns
numeric_ranges = {
    "Age": (0, 120),
    "Tenure": (0, None),
    "MonthlyCharges": (0, None),
    "TotalCharges": (0, None)
}

# COMMAND ----------

for col in numeric_ranges.keys():
    if not pd.api.types.is_numeric_dtype(data[col]):
        validation_results.append(f"{col} should be numeric but found {data[col].dtype}.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Range checks

# COMMAND ----------

for col, (min_val, max_val) in numeric_ranges.items():
    invalid_mask = (data[col] < min_val) if min_val is not None else pd.Series(False, index=data.index)
    if max_val is not None:
        invalid_mask |= (data[col] > max_val)
    if invalid_mask.any():
        validation_results.append(f"{col} has values outside range {min_val}-{max_val}: {data.loc[invalid_mask, col].tolist()}")
    else:
        validation_results.append(f" {col} values are within range {min_val}-{max_val}.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Allowed categorical values

# COMMAND ----------

# Allowed categorical values
allowed_values = {
    "Gender": ["M", "F"],
    "ContractType": ["Month-to-Month", "One Year", "Two Year"],
    "PaymentMethod": ["Electronic Check", "Mailed Check", "Bank Transfer", "Credit Card"]
}

# COMMAND ----------

for col, allowed in allowed_values.items():
    invalid_cats = set(data[col].unique()) - set(allowed)
    if invalid_cats:
        validation_results.append(f" {col} contains unexpected values: {invalid_cats}")
    else:
        validation_results.append(f" {col} values match allowed categories.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualizations

# COMMAND ----------

# MAGIC %md
# MAGIC #### Numeric distributions

# COMMAND ----------

num_cols = data.select_dtypes(include=[np.number]).columns
for col in num_cols:
    plt.figure(figsize=(6, 4))
    sns.histplot(data[col], kde=True, bins=5)
    plt.title(f"Distribution of {col}")
    plt.show()
    plt.close()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Categorical counts

# COMMAND ----------

cat_cols = data.select_dtypes(exclude=[np.number]).columns
for col in cat_cols:
    plt.figure(figsize=(6, 4))
    sns.countplot(x=data[col])
    plt.title(f"Value Counts of {col}")
    plt.show()
    plt.close()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Validation Summary

# COMMAND ----------

for observation in validation_results:
  print(observation)
