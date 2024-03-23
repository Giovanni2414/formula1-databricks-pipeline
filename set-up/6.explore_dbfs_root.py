# Databricks notebook source
# MAGIC %md
# MAGIC ### Explore DBFS root
# MAGIC 1. List all folders in DBFS root
# MAGIC 2. Interact with DBFS file exporer
# MAGIC 3. Upload file to DBFS root

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/"))

# COMMAND ----------

display(spark.read.csv("/FileStore/circuits.csv"))

# COMMAND ----------

