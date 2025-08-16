# Databricks notebook source
# DBTITLE 1,Deltalake connection Details
catalog_name = 'dmml'
schema_name = 'raw'
table_inter = 'raw_customer_churn_sf_temp'

# COMMAND ----------

# DBTITLE 1,Snowflake connetion details
# Define Snowflake connection details
snowflake_url = "roxlfst-jc21920.snowflakecomputing.com"
snowflake_user = "DHARAN"
snowflake_password = 'Yasodharan@420024#'
snowflake_warehouse = "COMPUTE_WH"
snowflake_role = "ACCOUNTADMIN"
snowflake_database = "dmml"
snowflake_schema = "CUSTOMER_CHURN"
snowflake_query_chunk_size = "1048576"

# COMMAND ----------

# DBTITLE 1,Create Spark Session
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Chrun Ingestion") \
    .getOrCreate()

# COMMAND ----------

# DBTITLE 1,Read data from snowflake as spark dataframe
sfOptions = {
  "sfURL":        snowflake_url,
  "sfUser":       snowflake_user,
  "sfPassword":   snowflake_password,     
  "sfWarehouse":  snowflake_warehouse,
  "sfRole":       snowflake_role,
  "sfDatabase":   snowflake_database,
  "sfSchema":     snowflake_schema,
  "sfQueryChunkSize": snowflake_query_chunk_size,
  "parallelism": "1"
}

df = (
  spark.read
       .format("net.snowflake.spark.snowflake")
       .options(**sfOptions)
       .option("query", "SELECT * FROM CUSTOMER_CHURN.CHURN_DATASET limit 100")
       .load()
)
# .option("dbtable", "CHURN_DATASET")  # the table you loaded
df.show(10)
df.printSchema()

# COMMAND ----------

# DBTITLE 1,select required features
df = df.select('CUSTOMERID','AGE','GENDER','TENURE','MONTHLYCHARGES','TOTALCHARGES','CONTRACTTYPE','PAYMENTMETHOD')

df = df.withColumnRenamed("CUSTOMERID", "CustomerID")
df = df.withColumnRenamed("AGE", "Age")
df = df.withColumnRenamed("GENDER", "Gender")
df = df.withColumnRenamed("TENURE", "Tenure")
df = df.withColumnRenamed("MONTHLYCHARGES", "MonthlyCharges")
df = df.withColumnRenamed("TOTALCHARGES", "TotalCharges")
df = df.withColumnRenamed("CONTRACTTYPE", "ContractType")
df = df.withColumnRenamed("PAYMENTMETHOD", "PaymentMethod")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# DBTITLE 1,Datatype correction
from pyspark.sql.types import IntegerType,DoubleType
df = df.withColumn("Age", df["Age"].cast(IntegerType())).withColumn("Tenure", df["Tenure"].cast(IntegerType()))

df = df.withColumn("MonthlyCharges", df["MonthlyCharges"].cast(DoubleType())).withColumn("TOTALCHARGES", df["TOTALCHARGES"].cast(DoubleType()))

# COMMAND ----------

# DBTITLE 1,Save Data in Deltalake Table
df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.{table_inter}")
