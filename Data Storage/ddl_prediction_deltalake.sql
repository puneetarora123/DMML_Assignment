-- Databricks notebook source
-- DBTITLE 1,Create Temp Table for Inference process output
CREATE TABLE IF NOT EXISTS dmml.datamart.inference_customer_churn_temp (
    CustomerID STRING COMMENT 'Customer unique identifier',
    Churn_Prediction STRING COMMENT 'Predicted churn label (Yes/No)',
    Churn_Confidence DOUBLE COMMENT 'Confidence score of prediction'
)
USING delta
COMMENT 'Temporary inference results for customer churn prediction';


-- COMMAND ----------

-- DBTITLE 1,Create Table to store Preciction Output along with input features
CREATE TABLE IF NOT EXISTS dmml.datamart.churn_prediction_output (
    CustomerID STRING NOT NULL COMMENT 'Customer unique identifier',
    Age INT COMMENT 'Age of customer',
    Gender STRING COMMENT 'Gender of customer',
    Tenure INT COMMENT 'Tenure in months',
    MonthlyCharges DOUBLE COMMENT 'Monthly charges',
    TotalCharges DOUBLE COMMENT 'Total charges',
    ContractType STRING COMMENT 'Type of contract',
    PaymentMethod STRING COMMENT 'Customer payment method',
    source STRING NOT NULL COMMENT 'Source system identifier',
    Churn STRING COMMENT 'Actual churn label (Yes/No)',
    Churn_Probability DOUBLE COMMENT 'Predicted probability of churn'
)
USING delta
COMMENT 'Datamart table for storing churn prediction output';

