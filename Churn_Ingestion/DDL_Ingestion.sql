-- Databricks notebook source
use catalog dmml;
create schema raw;

-- COMMAND ----------

create schema datamart;

-- COMMAND ----------

CREATE or REPLACE TABLE dmml.raw.raw_customer_churn_hf_temp (
    CustomerID      VARCHAR(10)  NOT NULL PRIMARY KEY,
    Age             INT            ,
    Gender          VARCHAR(10)    ,
    Tenure          INT            ,
    MonthlyCharges  DOUBLE ,
    TotalCharges    Double  ,
    ContractType    VARCHAR(20) ,
    PaymentMethod   VARCHAR(50) 
);

-- COMMAND ----------

CREATE OR REPLACE TABLE dmml.raw.raw_customer_churn_sf_temp (
     CustomerID      VARCHAR(10)  NOT NULL PRIMARY KEY,
    Age             INT            ,
    Gender          VARCHAR(10)    ,
    Tenure          INT            ,
    MonthlyCharges  DOUBLE ,
    TotalCharges    Double  ,
    ContractType    VARCHAR(20) ,
    PaymentMethod   VARCHAR(50) 
);

-- COMMAND ----------

CREATE OR REPLACE TABLE dmml.raw.raw_customer_churn_master (
    CustomerID      VARCHAR(10)  NOT NULL PRIMARY KEY,
    Age             INT            ,
    Gender          VARCHAR(10)    ,
    Tenure          INT            ,
    MonthlyCharges  DOUBLE ,
    TotalCharges    Double  ,
    ContractType    VARCHAR(20) ,
    PaymentMethod   VARCHAR(50) ,
    source          VARCHAR(10)    NOT NULL
);

-- COMMAND ----------

CREATE OR REPLACE TABLE dmml.raw.raw_customer_churn_temp (
    CustomerID      VARCHAR(10)  NOT NULL PRIMARY KEY,
    Age             INT            ,
    Gender          VARCHAR(10)    ,
    Tenure          INT            ,
    MonthlyCharges  DOUBLE ,
    TotalCharges    Double  ,
    ContractType    VARCHAR(20) ,
    PaymentMethod   VARCHAR(50) ,
    source          VARCHAR(10)    NOT NULL
);
