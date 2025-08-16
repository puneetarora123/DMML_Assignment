-- Databricks notebook source
DESCRIBE HISTORY dmml.raw.raw_customer_churn_hf_temp;

-- COMMAND ----------

DESCRIBE HISTORY dmml.raw.raw_customer_churn_hf_temp;

-- COMMAND ----------

DESCRIBE HISTORY dmml.raw.raw_customer_churn_master;

-- COMMAND ----------

DESCRIBE HISTORY dmml.raw.raw_customer_churn_sf_temp;

-- COMMAND ----------

DESCRIBE HISTORY dmml.raw.raw_customer_churn_temp;

-- COMMAND ----------

DESCRIBE HISTORY dmml.raw.churn_training_data;

-- COMMAND ----------

DESCRIBE HISTORY dmml.datamart.churn_prediction_output;

-- COMMAND ----------

DESCRIBE HISTORY dmml.datamart.customer_churn_processed;

-- COMMAND ----------

DESCRIBE HISTORY dmml.datamart.inference_customer_churn_temp;

-- COMMAND ----------

SELECT 'dmml.raw.raw_customer_churn_hf_temp' AS table_name, * 
FROM DESCRIBE HISTORY dmml.raw.raw_customer_churn_hf_temp
UNION ALL
SELECT 'dmml.raw.raw_customer_churn_master' AS table_name, * 
FROM DESCRIBE HISTORY dmml.raw.raw_customer_churn_master
UNION ALL
SELECT 'dmml.raw.raw_customer_churn_sf_temp' AS table_name, * 
FROM DESCRIBE HISTORY dmml.raw.raw_customer_churn_sf_temp
UNION ALL
SELECT 'dmml.raw.raw_customer_churn_temp' AS table_name, * 
FROM DESCRIBE HISTORY dmml.raw.raw_customer_churn_temp
UNION ALL
SELECT 'dmml.raw.churn_training_data' AS table_name, * 
FROM DESCRIBE HISTORY dmml.raw.churn_training_data
UNION ALL
SELECT 'dmml.datamart.churn_prediction_output' AS table_name, * 
FROM DESCRIBE HISTORY dmml.datamart.churn_prediction_output
UNION ALL
SELECT 'dmml.datamart.customer_churn_processed' AS table_name, * 
FROM DESCRIBE HISTORY dmml.datamart.customer_churn_processed
UNION ALL
SELECT 'dmml.datamart.inference_customer_churn_temp' AS table_name, * 
FROM DESCRIBE HISTORY dmml.datamart.inference_customer_churn_temp;

