# Databricks notebook source
# ===============================
# Script Name: merge_customer_churn_tables.py
# Purpose: Merge customer churn data from HF and SF sources
# into a single master table with source column
# ===============================

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# COMMAND ----------

# DBTITLE 1,Create Spark session
spark = SparkSession.builder \
    .appName("Merge Customer Churn Tables") \
    .getOrCreate()


# COMMAND ----------

hf_table = "dmml.raw.raw_customer_churn_hf_temp"
sf_table = "dmml.raw.raw_customer_churn_sf_temp"
merged_master_table = "dmml.raw.raw_customer_churn_master"
merged_temp_table = "dmml.raw.raw_customer_churn_temp"

# COMMAND ----------

# DBTITLE 1,Read Hugging Face data from deltalake raw table
# Read HF table
df_hf = spark.read.format("delta").table(hf_table)
print("HF Table Schema:")
df_hf.printSchema()
print("HF Table Sample Data:")
df_hf.show(5)

# COMMAND ----------

# DBTITLE 1,Read snowflake data from deltalake raw table
# Read SF table
df_sf = spark.read.format("delta").table(sf_table)
print("SF Table Schema:")
df_sf.printSchema()
print("SF Table Sample Data:")
df_sf.show(5)

# COMMAND ----------

# DBTITLE 1,#Add source column
df_hf = df_hf.withColumn("source", lit("HugingFace"))
df_sf = df_sf.withColumn("source", lit("Snowflake"))

# COMMAND ----------

# DBTITLE 1,Merge the tables
# Concatenate dataframes
df_merged = df_hf.unionByName(df_sf)

# Optional: handle duplicates if CustomerID exists in both
# Keep HF version if duplicate exists
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("CustomerID").orderBy("source")  # HF first alphabetically
df_final = df_merged.withColumn("row_num", row_number().over(window_spec)) \
                    .filter("row_num = 1") \
                    .drop("row_num")

# COMMAND ----------

df_final.show()

# COMMAND ----------

# DBTITLE 1,write to temp merged table
df_final.write.format("delta").mode("append").saveAsTable(merged_temp_table)

print(f"Successfully merged tables into {merged_temp_table}")
df_final.show(10)


# COMMAND ----------

# DBTITLE 1,Write to master table
df_final.write.format("delta").mode("append").saveAsTable(merged_master_table)

print(f"Successfully merged tables into {merged_master_table}")
df_final.show(10)

