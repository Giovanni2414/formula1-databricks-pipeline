# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

results_df = spark.read.table("f1_processed.results").filter(f"file_date = '{v_file_date}'").withColumnRenamed("time", "race_time").withColumnRenamed("race_id", "result_race_id").withColumnRenamed("file_date", "result_file_date")

# COMMAND ----------

display(results_df)

# COMMAND ----------

races_df = spark.read.table("f1_processed.races").withColumnRenamed("name", "race_name").withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

drivers_df = spark.read.table("f1_processed.drivers").withColumnRenamed("name", "driver_name").withColumnRenamed("number", "driver_number").withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_df = spark.read.table("f1_processed.constructors").withColumnRenamed("name", "team")

# COMMAND ----------

    circuits_df = spark.read.table("f1_processed.circuits").withColumnRenamed("location", "circuit_location")

# COMMAND ----------

results_races_df = results_df.join(races_df, results_df.result_race_id == races_df.race_id)

# COMMAND ----------

results_circuits_df = results_races_df.join(circuits_df, circuits_df.circuit_id == results_races_df.circuit_id)

# COMMAND ----------

results_drivers_df = results_circuits_df.join(drivers_df, drivers_df.driver_id == results_circuits_df.driver_id)

# COMMAND ----------

results_constructor_df = results_drivers_df.join(constructors_df, constructors_df.constructor_id == results_drivers_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

# COMMAND ----------

results_final_df = results_constructor_df.withColumn("created_date", current_timestamp()).select(col("race_year"), col("race_name"), col("race_date"), col("circuit_location"), col("driver_name"), col("driver_number"), col("driver_nationality"), col("team"), col("grid"), col("fastest_lap"), col("race_time"), col("points"), col("position"), col("result_race_id"), col("result_file_date")).withColumnRenamed("result_file_date","file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data into lakehouse

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_presentation
# MAGIC LOCATION '/mnt/formula1dlgio/presentation';

# COMMAND ----------

#results_final_df.write.saveAsTable("f1_presentation.race_results", format="delta", mode="overwrite")
merge_condition = "tgt.driver_name = src.driver_name AND tgt.result_race_id = src.result_race_id"
merge_delta_data(results_final_df, "f1_presentation", "race_results", presentation_folder_path, merge_condition, "result_race_id")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analyze the result

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT result_race_id, COUNT(1)
# MAGIC FROM f1_presentation.race_results
# MAGIC GROUP BY result_race_id
# MAGIC ORDER BY result_race_id DESC;