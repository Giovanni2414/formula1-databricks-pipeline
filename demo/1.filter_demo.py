# Databricks notebook source
# MAGIC %run "../includes/configuration/"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_filtered_df = races_df.filter((col("round") == 1) & (col("circuit_id") == 3))

# COMMAND ----------

races_filtered_df.show(10)