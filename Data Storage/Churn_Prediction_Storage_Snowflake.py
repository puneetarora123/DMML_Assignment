# Databricks notebook source
# DBTITLE 1,Deltalake properties
catalog_name = 'dmml'
schema_name = 'datamart'
prediction_dl_table = 'churn_prediction_output'

# COMMAND ----------

# DBTITLE 1,Snowflake connection Details
snowflake_url = "roxlfst-jc21920.snowflakecomputing.com"
snowflake_user = "DHARAN"
snowflake_password = ''
snowflake_warehouse = "COMPUTE_WH"
snowflake_role = "ACCOUNTADMIN"
snowflake_database = "dmml"
snowflake_schema = "CUSTOMER_CHURN"
snowflake_query_chunk_size = "1048576"

# COMMAND ----------

# DBTITLE 1,Creating Snowlfake connecton object
# Snowflake connection options
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

# COMMAND ----------

# DBTITLE 1,Load Delta table from Deltalake
delta_df = spark.table(f"{catalog_name}.{schema_name}.{prediction_dl_table}")
display(delta_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Write data into Snowflake table
(
    delta_df.write.format("net.snowflake.spark.snowflake")
    .options(**sfOptions)
    .option("dbtable", "CHURN_PREDICTION_OUTPUT")
    .mode("overwrite")
    .save()
)
