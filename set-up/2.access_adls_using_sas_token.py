# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using SAS token
# MAGIC 1. Set the spark configuration for SAS token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlgio.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dlgio.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dlgio.dfs.core.windows.net", "sp=rl&st=2024-03-01T03:28:47Z&se=2024-03-31T11:28:47Z&spr=https&sv=2022-11-02&sr=c&sig=ymyaPoz%2BL9NDDBrnrN3ma4l0wfzN9kgPzsLqt9qwZVg%3D")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlgio.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlgio.dfs.core.windows.net/circuits.csv"))