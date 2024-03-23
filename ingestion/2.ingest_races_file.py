# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingesting races.csv file
# MAGIC ##### Read the file using the reader API of spark

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create the required schema

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

from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType

# COMMAND ----------

races_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/races.csv", header=True, schema=races_schema)
races_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Give a first lookup to the data and check its types

# COMMAND ----------

display(races_df.describe())

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Select the required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_selected_df = races_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("date"), col("time"))
races_selected_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Rename the existing columns based on requirements

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id").withColumnRenamed("year", "race_year").withColumnRenamed("circuitId", "circuit_id").withColumn("file_date", lit(v_file_date))
races_renamed_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating and adding new columns
# MAGIC ###### Creating the composed column of 'race_timestamp' of 'date' and 'time' present columns

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, lit

# COMMAND ----------

races_transformed_df = races_renamed_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')).withColumn("data_source", lit(v_data_source))
races_transformed_df = races_transformed_df.select("race_id", "race_year", "round", "circuit_id", "name", "race_timestamp")
races_transformed_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Adding the new column for audit the data

# COMMAND ----------

races_final_df = add_ingestion_date(races_transformed_df)
races_final_df = races_final_df.withColumn("file_date", lit(v_file_date))
races_final_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write the final df into the data lake as parquet file

# COMMAND ----------

races_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")