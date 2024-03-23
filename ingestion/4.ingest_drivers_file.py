# Databricks notebook source
# MAGIC %md
# MAGIC ##### Ingesting drivers.json with nested json objects file
# MAGIC ##### Read the file to create a spark dataframe

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

# COMMAND ----------

drivers_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema, True),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/drivers.json", schema=drivers_schema)

# COMMAND ----------

drivers_df.show(10)

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Rename columns and add new ones

# COMMAND ----------

drivers_renamed_df = drivers_df.withColumnRenamed("driverId", "driver_id").withColumnRenamed("driverRef", "driver_ref")

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, current_timestamp

# COMMAND ----------

drivers_added_df = drivers_renamed_df.withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

drivers_added_df = add_ingestion_date(drivers_added_df)

# COMMAND ----------

drivers_added_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Drop the required columns

# COMMAND ----------

drivers_final_df = drivers_added_df.drop(col("url"))
drivers_final_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Writte the data into the processed layer as parquet file

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")