# Databricks notebook source
# MAGIC %md
# MAGIC ##### Ingest pitstops.json multiline file
# MAGIC ##### Create schema and read file from mount to get spark dataframe

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# COMMAND ----------

pitstops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("stop", IntegerType(), False),
    StructField("lap", IntegerType(), False),
    StructField("time", StringType(), False),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pitstops_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json", schema=pitstops_schema, multiLine=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Transform columns (rename and add columns)

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

pitstops_renamed_df = pitstops_df.withColumnRenamed("raceId", "race_id").withColumnRenamed("driverId", "driver_id").withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

pitstops_final_df = add_ingestion_date(pitstops_renamed_df)

# COMMAND ----------

#pitstops_final_df.write.saveAsTable("f1_processed.pit_stops", format="delta", mode="overwrite")
#save_append_to_data_lake("f1_processed.pit_stops", pitstops_final_df)
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop"
merge_delta_data(pitstops_final_df, "f1_processed", "pit_stops", processed_folder_path, merge_condition, "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")