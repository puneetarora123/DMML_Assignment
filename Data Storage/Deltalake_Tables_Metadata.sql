-- Databricks notebook source
use catalog dmml;

SHOW SCHEMAS

-- COMMAND ----------

show tables in raw

-- COMMAND ----------

show tables in datamart

-- COMMAND ----------

-- DBTITLE 1,Show schema
DESCRIBE DETAIL dmml.raw.raw_customer_churn_hf_temp;

DESCRIBE DETAIL dmml.raw.raw_customer_churn_master;

DESCRIBE DETAIL dmml.raw.raw_customer_churn_sf_temp;

DESCRIBE DETAIL dmml.raw.raw_customer_churn_temp;

DESCRIBE DETAIL dmml.raw.churn_training_data;

DESCRIBE DETAIL dmml.datamart.churn_prediction_output;

DESCRIBE DETAIL dmml.datamart.customer_churn_processed;

DESCRIBE DETAIL dmml.datamart.inference_customer_churn_temp;

-- COMMAND ----------

-- DBTITLE 1,see Delta table properties
DESCRIBE DETAIL dmml.raw.raw_customer_churn_hf_temp;

DESCRIBE DETAIL dmml.raw.raw_customer_churn_master;

DESCRIBE DETAIL dmml.raw.raw_customer_churn_sf_temp;

DESCRIBE DETAIL dmml.raw.raw_customer_churn_temp;

DESCRIBE DETAIL dmml.raw.churn_training_data;

DESCRIBE DETAIL dmml.datamart.churn_prediction_output;

DESCRIBE DETAIL dmml.datamart.customer_churn_processed;

DESCRIBE DETAIL dmml.datamart.inference_customer_churn_temp;

-- COMMAND ----------

-- DBTITLE 1,Get row counts for all tables in RAW schema
SELECT 'raw_customer_churn_hf_temp' AS table_name, COUNT(*) AS row_count FROM dmml.raw.raw_customer_churn_hf_temp
UNION ALL
SELECT 'raw_customer_churn_master' , COUNT(*) FROM dmml.raw.raw_customer_churn_master
UNION ALL
SELECT 'raw_customer_churn_sf_temp' , COUNT(*) FROM dmml.raw.raw_customer_churn_sf_temp
UNION ALL
SELECT 'raw_customer_churn_temp' , COUNT(*) FROM dmml.raw.raw_customer_churn_temp
UNION ALL
SELECT 'churn_training_data' , COUNT(*) FROM dmml.raw.churn_training_data;

-- COMMAND ----------

-- DBTITLE 1,Get row counts for all tables in DATAMART schema
SELECT 'churn_prediction_output' AS table_name, COUNT(*) AS row_count FROM dmml.datamart.churn_prediction_output
UNION ALL
SELECT 'customer_churn_processed' , COUNT(*) FROM dmml.datamart.customer_churn_processed
UNION ALL
SELECT 'inference_customer_churn_temp' , COUNT(*) FROM dmml.datamart.inference_customer_churn_temp;

