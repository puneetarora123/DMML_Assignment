# Databricks notebook source
# DBTITLE 1,Install Libraries
pip install fsspec huggingface_hub

# COMMAND ----------

# DBTITLE 1,Configurations
catalog_name = 'dmml'
schema_name = 'raw'
table_inter = 'raw_customer_churn_hf_temp'
table_master = 'raw_customer_churn_hf_master'

# COMMAND ----------

# DBTITLE 1,Create Spark Session
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Chrun Ingestion") \
    .getOrCreate()

# COMMAND ----------

# DBTITLE 1,Load Data From HuggingFace web portal
import pandas as pd

df_pd = pd.read_csv("hf://datasets/aai510-group1/telco-customer-churn/train.csv" )   

# COMMAND ----------

# DBTITLE 1,Create Spark Dataframe & select required columns
df = spark.createDataFrame(df_pd)  
df = df.select('Customer ID','Age','Gender','Tenure in Months','Monthly Charge','Total Charges','Contract','Payment Method')

df = df.withColumnRenamed("Customer ID", "CustomerID").\
        withColumnRenamed("Tenure in Months", "Tenure").\
        withColumnRenamed("Monthly Charge", "MonthlyCharges").\
        withColumnRenamed("Total Charges", "TotalCharges").\
        withColumnRenamed("Contract", "ContractType").\
        withColumnRenamed("Payment Method", "PaymentMethod")

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe dmml.raw.raw_customer_churn_hf_temp

# COMMAND ----------

from pyspark.sql.types import IntegerType
df = df.withColumn("Age", df["Age"].cast(IntegerType())).withColumn("Tenure", df["Tenure"].cast(IntegerType()))

# COMMAND ----------

# DBTITLE 1,Save data into Deltalake Table
df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.{table_inter}")
