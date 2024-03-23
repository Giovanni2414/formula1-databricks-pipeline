# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits").withColumnRenamed("name", "circuit_name")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter(col("race_year") == 2019)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id).select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

