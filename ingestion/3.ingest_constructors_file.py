# Databricks notebook source
# MAGIC %md
# MAGIC ##### Ingest constructors.json file
# MAGIC ##### Get the spark DataFrame using the reader API

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Creting the Schema based on DDL

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

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/constructors.json", schema=constructors_schema)

# COMMAND ----------

constructors_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Drop the unnecessary columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructors_df = constructors_df.drop(col("url"))

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Renaming the columns and adding the new columns for audit purposes

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructors_final_df = constructors_df.withColumnRenamed("constructorId", "constructor_id").withColumnRenamed("constructorRef", "constructor_ref").withColumn("ingestion_date", current_timestamp()).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))

# COMMAND ----------

constructors_final_df = add_ingestion_date(constructors_final_df)

# COMMAND ----------

constructors_final_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Writing the parquet file

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")