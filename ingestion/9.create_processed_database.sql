-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
MANAGED LOCATION "abfss://processed@formula1dlgio.dfs.core.windows.net/"

-- COMMAND ----------

DESCRIBE DATABASE f1_processed;

-- COMMAND ----------

