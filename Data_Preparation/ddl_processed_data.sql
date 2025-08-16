-- Databricks notebook source
CREATE TABLE IF NOT EXISTS dmml.datamart.customer_churn_processed (
    CustomerID STRING COMMENT 'Customer unique identifier',
    Age DOUBLE COMMENT 'Age of customer',
    Tenure DOUBLE COMMENT 'Tenure in months',
    MonthlyCharges DOUBLE COMMENT 'Monthly charges',
    TotalCharges DOUBLE COMMENT 'Total charges',
    SeniorCitizenFlag INT COMMENT 'Flag if customer is a senior citizen',
    LongTenureFlag INT COMMENT 'Flag if customer has long tenure',
    Gender_male INT COMMENT 'Encoded gender: male=1, else 0',
    ContractType_one_year INT COMMENT 'One-year contract flag',
    ContractType_two_year INT COMMENT 'Two-year contract flag',
    PaymentMethod_credit_card INT COMMENT 'Credit card payment flag',
    PaymentMethod_mailed_check INT COMMENT 'Mailed check payment flag'
)
USING delta
COMMENT 'Customer churn feature store table';
