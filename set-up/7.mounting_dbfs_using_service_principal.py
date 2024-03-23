# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Microsoft Entra ID
# MAGIC 1. Register Azure AD Aplication / Service Principal
# MAGIC 2. Generate password/secret for our application
# MAGIC 3. Set spark configuration with App/ClientId, Directory/Tenant & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

def mount_dbfs(container_name, storage_account):
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "55e17a4e-cb5f-4965-9602-d34ba5808623",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get("formula1-scope", "formula1-app-client-secret"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/9482878d-5e6d-4f6a-b37a-db1da9ea1c90/oauth2/token"}
    
    current_mounted = [mount.mountPoint for mount in dbutils.fs.mounts()]
    if f"/mnt/{storage_account}/{container_name}" in current_mounted:
        dbutils.fs.unmount(f"/mnt/{storage_account}/{container_name}")

    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account}/{container_name}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_dbfs("demo", "formula1dlgio")
mount_dbfs("raw", "formula1dlgio")
mount_dbfs("presentation", "formula1dlgio")
mount_dbfs("processed", "formula1dlgio")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dlgio/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dlgio/demo/circuits.csv"))