# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, lower, regexp_replace, mean, stddev
from pyspark.sql.types import IntegerType, DoubleType, StringType

# COMMAND ----------

# Create Spark session
spark = SparkSession.builder.appName("ChurnDataCleaning").getOrCreate()

# COMMAND ----------

merged_temp_table = "dmml.raw.raw_customer_churn_temp"
processed_table = 'dmml.datamart.customer_churn_processed'


# COMMAND ----------

# DBTITLE 1,Load the raw table (both sources - merged data)
df = spark.table(merged_temp_table)

# COMMAND ----------

# DBTITLE 1,Keep the required features required for inference
feature_cols = ['CustomerID','Age','Gender','Tenure','MonthlyCharges','TotalCharges','ContractType','PaymentMethod']

df = df.select(feature_cols)

# COMMAND ----------

# DBTITLE 1,Remove Duplicates
df = df.dropDuplicates()

# COMMAND ----------

# DBTITLE 1,standardize columns
string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]

# Lowercase, trim spaces for all string columns
for col_name in string_cols:
    df = df.withColumn(col_name, trim(lower(col(col_name))))

# COMMAND ----------

# DBTITLE 1,Fix known categorical inconsistencies
# Fix known categorical inconsistencies
df = df.withColumn("Gender", 
                   when(col("Gender").isin("male", "m"), "male")
                   .when(col("Gender").isin("female", "f"), "female")
                   .otherwise(col("Gender")))

# COMMAND ----------

df

# COMMAND ----------

# DBTITLE 1,Handle Missing Values- Numerical Imputation
numeric_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, (IntegerType, DoubleType))]

# Numeric: Fill with median
for col_name in numeric_cols:
    median_val = df.approxQuantile(col_name, [0.5], 0.01)[0]
    df = df.na.fill({col_name: median_val})

# COMMAND ----------

# DBTITLE 1,Handle Missing Values- Categorical Imputation
categorical_cols = [c for c in df.columns if c not in numeric_cols]

# Categorical: Fill with mode
for col_name in categorical_cols:
    mode_val = df.groupBy(col_name).count().orderBy(col("count").desc()).first()[0]
    df = df.na.fill({col_name: mode_val})

# COMMAND ----------

# DBTITLE 1,Ensure correct data types
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, StringType

df = df.withColumn("CustomerID", col("CustomerID").cast(StringType()))
df = df.withColumn("Age", col("Age").cast(IntegerType()))
df = df.withColumn("Gender", col("Gender").cast(StringType()))
df = df.withColumn("Tenure", col("Tenure").cast(IntegerType()))
df = df.withColumn("MonthlyCharges", col("MonthlyCharges").cast(DoubleType()))
df = df.withColumn("TotalCharges", col("TotalCharges").cast(DoubleType()))
df = df.withColumn("ContractType", col("ContractType").cast(StringType()))
df = df.withColumn("PaymentMethod", col("PaymentMethod").cast(StringType()))

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# DBTITLE 1,Remove extreme outliers
from pyspark.sql.functions import mean, stddev, col, abs as F_abs

for col_name in ["MonthlyCharges", "TotalCharges", "Age", "Tenure"]:
    stats = df.select(mean(col_name).alias("mean"), stddev(col_name).alias("std")).collect()[0]
    mean_val, std_val = stats["mean"], stats["std"]
    df = df.filter((F_abs((col(col_name) - mean_val) / std_val)) < 3)

display(df)

# COMMAND ----------

# DBTITLE 1,Derive New Features
# Senior Citizen flag
df = df.withColumn("SeniorCitizenFlag", when(col("Age") >= 60, 1).otherwise(0))

# Flag for long tenure customers
df = df.withColumn("LongTenureFlag", when(col("Tenure") >= 24, 1).otherwise(0))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Scale Numeric Features
X = df.toPandas()

features_to_scale = ['Age', 'Tenure', 'MonthlyCharges', 'TotalCharges']
from sklearn.preprocessing import StandardScaler, MinMaxScaler, LabelEncoder
scaler = StandardScaler()
X[features_to_scale] = scaler.fit_transform(X[features_to_scale])

# COMMAND ----------

# DBTITLE 1,Encode Categorical columns
# One Hot Encoding
categorical_cols = ['Gender', 'ContractType', 'PaymentMethod']

import pandas as pd

X = pd.get_dummies(X, columns=categorical_cols, drop_first=True)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

# Define the schema based on the data types
schema = StructType([
    StructField("CustomerID", StringType(), True),
    StructField("Age", DoubleType(), True),
    StructField("Tenure", DoubleType(), True),
    StructField("MonthlyCharges", DoubleType(), True),
    StructField("TotalCharges", DoubleType(), True),
    StructField("SeniorCitizenFlag", IntegerType(), True),
    StructField("LongTenureFlag", IntegerType(), True),
    StructField("Gender_male", IntegerType(), True),
    StructField("ContractType_one year", IntegerType(), True),
    StructField("ContractType_two year", IntegerType(), True),
    StructField("PaymentMethod_credit card", IntegerType(), True),
    StructField("PaymentMethod_mailed check", IntegerType(), True)
])

# Create the DataFrame with the specified schema
df_scaled = spark.createDataFrame(X, schema)

df_scaled.createOrReplaceTempView("tbl_scaled")

# COMMAND ----------

# Replace spaces with underscores in column names
df_scaled = df_scaled.toDF(*(c.replace(' ', '_') for c in df_scaled.columns))

df_scaled.createOrReplaceTempView("tbl_scaled")

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe tbl_scaled

# COMMAND ----------

spark.sql(f'create or replace table {processed_table} as select * from tbl_scaled')

# COMMAND ----------

spark.sql(f'select * from {processed_table}').display()
