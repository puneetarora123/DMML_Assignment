# Databricks notebook source
# DBTITLE 1,Deltalake COnfiguration
catalog = 'dmml'
raw_schema = 'raw'
datamart_schema = 'datamart'

raw_input_temp_table = "raw_customer_churn_temp"
inference_output_temp_table = "inference_customer_churn_temp"

output_prediction_table_deltalake = 'churn_prediction_output'

# COMMAND ----------

# DBTITLE 1,Read the Raw Table & Inference output tables from deltalake
df_raw = spark.table(f'{catalog}.{raw_schema}.{raw_input_temp_table}')
df_pred = spark.table(f'{catalog}.{datamart_schema}.{inference_output_temp_table}')

# COMMAND ----------

df_pred.printSchema()

# COMMAND ----------

df_raw.printSchema()

# COMMAND ----------

df_pred.count()

# COMMAND ----------

df_raw.count()

# COMMAND ----------

# DBTITLE 1,Standardize COlumn names
df_pred = df_pred.withColumnRenamed('Churn_Prediction', 'Churn')
df_pred = df_pred.withColumnRenamed('Churn_Confidence', 'Churn_Probability')

# COMMAND ----------

# DBTITLE 1,Join inference output with original input data
prediction_df = df_raw.join(df_pred, on='CustomerID', how='left')
prediction_df.printSchema()

# COMMAND ----------

prediction_df.count()

# COMMAND ----------

display(prediction_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Push the Data to the Deltalake output Prediction Table
prediction_df.write.mode("overwrite").saveAsTable(f"{catalog}.{datamart_schema}.{output_prediction_table_deltalake}")

# COMMAND ----------

spark.sql(f'select * from {catalog}.{datamart_schema}.{output_prediction_table_deltalake} limit 10').show()
