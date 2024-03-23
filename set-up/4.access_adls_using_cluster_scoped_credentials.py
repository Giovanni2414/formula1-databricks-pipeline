# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Microsoft Entra ID
# MAGIC 1. Register Azure AD Aplication / Service Principal
# MAGIC 2. Generate password/secret for our application
# MAGIC 3. Set spark configuration with App/ClientId, Directory/Tenant & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlgio.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlgio.dfs.core.windows.net/circuits.csv"))