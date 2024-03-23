-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * 
FROM drivers 
WHERE nationality = 'British'
LIMIT 10;

-- COMMAND ----------

DESC drivers;

-- COMMAND ----------

