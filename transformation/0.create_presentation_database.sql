-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_presentation
MANAGED LOCATION "abfss://presentation@formula1dlgio.dfs.core.windows.net"

-- COMMAND ----------

CREATE DATABASE f1_presentation
LOCATION "dbfs:/user/hive/"

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;