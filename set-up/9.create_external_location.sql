-- Databricks notebook source
CREATE EXTERNAL LOCATION IF NOT EXISTS  dataengineeringudemy_rg_raw 
URL 'abfss://raw@formula1dlgio.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databrickscourse-ext-storage-credential-formula1`)

-- COMMAND ----------

