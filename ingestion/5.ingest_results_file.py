# Databricks notebook source
# MAGIC %md
# MAGIC ##### Ingest results.json file
# MAGIC ##### Create the schema and read the data to get a spark dataframe

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("constructorId", IntegerType(), False),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), False),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), False),
    StructField("positionOrder", IntegerType(), False),
    StructField("points", FloatType(), False),
    StructField("laps", IntegerType(), False),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", StringType(), True),
    StructField("statusId", IntegerType(), False),
])

# COMMAND ----------

results_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/results.json", schema=results_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Rename columns

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId", "result_id").withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("positionText", "position_text").withColumnRenamed("positionOrder", "position_order").withColumnRenamed("fastestLap", "fastest_lap").withColumnRenamed("fastestLapTime", "fastest_lap_time").withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Drop unwanted columns

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

result_dropped_df = results_renamed_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Drop duplicates

# COMMAND ----------

result_dropped_df = result_dropped_df.dropDuplicates(["race_id", "driver_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Add new columns
# MAGIC 1. Auditing column (ingestion_date : timestamp)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_final_df = add_ingestion_date(result_dropped_df).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write the file within the processed layer as parquet file

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Incremental load method 1

# COMMAND ----------

#for race_id_list in results_final_df.select("race_id").distinct().collect():
#    if spark._jsparkSession.catalog().tableExists("f1_processed.results"):
#        spark.sql(f"DELETE FROM f1_processed.results WHERE race_id = {race_id_list.race_id}")

# COMMAND ----------

#results_final_df.write.saveAsTable("f1_processed.results", format="delta", mode="append")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Incremental load method 2

# COMMAND ----------

# spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

# results_final_df = results_final_df.select("result_id","driver_id","constructor_id","number","grid","position","position_text","position_order","points","laps","time","milliseconds","fastest_lap","rank","fastest_lap_time","fastest_lap_speed","data_source","file_date","ingestion_date","race_id")

# COMMAND ----------

# spark._jsparkSession.catalog().tableExists("f1_processed.results")

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#    if spark._jsparkSession.catalog().tableExists("f1_processed.results"):
#        results_final_df.write.mode("overwrite").insertInto("f1_processed.results")
#    else:
#        results_final_df.write.saveAsTable("f1_processed.results", format="delta", mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Delta Lake Writing method

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_final_df, "f1_processed", "results", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Analyzing data to find duplicates

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, driver_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id, driver_id
# MAGIC HAVING COUNT(1) > 1
# MAGIC ORDER BY race_id, driver_id DESC;

# COMMAND ----------

dbutils.notebook.exit("Success")