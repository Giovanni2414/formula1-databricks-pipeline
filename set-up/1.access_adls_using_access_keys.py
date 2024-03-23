# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark configuration for fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dlgio.dfs.core.windows.net",
    dbutils.secrets.get('formula1-scope', 'formula1-dl-key1')
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlgio.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlgio.dfs.core.windows.net/circuits.csv"))