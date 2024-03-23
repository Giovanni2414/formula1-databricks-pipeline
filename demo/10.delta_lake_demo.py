# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 2. Write data to delta lake (external table)
# MAGIC 3. Read data from delta lake (Table)
# MAGIC 4. Read data from delta lake (File)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC MANAGED LOCATION 'abfss://demo@formula1dlgio.dfs.core.windows.net/'

# COMMAND ----------

results_df = spark.read.option("inferSchema", True).json("/mnt/formula1dlgio/raw/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("abfss://demo@formula1dlgio.dfs.core.windows.net/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://demo@formula1dlgio.dfs.core.windows.net/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_external;

# COMMAND ----------

results_external_df = spark.read.format("delta").load("abfss://demo@formula1dlgio.dfs.core.windows.net/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned_managed;

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").save("abfss://demo@formula1dlgio.dfs.core.windows.net/results_partitioned_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.results_partitioned_external
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://demo@formula1dlgio.dfs.core.windows.net/results_partitioned_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_partitioned_external;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC TABLE EXTENDED f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC TABLE EXTENDED f1_demo.results_external;

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Update Delta Table
# MAGIC 2. Delete from Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC   SET points = 11 - position
# MAGIC   WHERE position <= 10;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath('...') # Cell pending for completition
deltaTable.update("position <= 10", { "points": "21 - position" })

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC   WHERE position <= 10;

# COMMAND ----------

# MAGIC %md
# MAGIC Upsert using merge

# COMMAND ----------

drivers_day1_df = spark.read.option("inferSchema", True).json("/mnt/formula1dlgio/raw/2021-03-28/drivers.json").filter("driverId <= 10").select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read.option("inferSchema", True).json("/mnt/formula1dlgio/raw/2021-03-28/drivers.json").filter("driverId BETWEEN 6 AND 15").select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3_df = spark.read.option("inferSchema", True).json("/mnt/formula1dlgio/raw/2021-03-28/drivers.json").filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 and 20").select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updateDate DATE
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Day 1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge AS tgt
# MAGIC USING drivers_day1 AS upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC     tgt.forename = upd.forename, 
# MAGIC     tgt.surname = upd.surname, 
# MAGIC     tgt.updateDate = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, CURRENT_TIMESTAMP)

# COMMAND ----------

# MAGIC %md
# MAGIC Day 2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge AS tgt
# MAGIC USING drivers_day2 AS upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC     tgt.forename = upd.forename, 
# MAGIC     tgt.surname = upd.surname, 
# MAGIC     tgt.updateDate = CURRENT_TIMESTAMP
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, CURRENT_TIMESTAMP)

# COMMAND ----------

# MAGIC %md
# MAGIC 1. History & Versioning
# MAGIC 2. Time Travel
# MAGIC 3. Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-03-18T22:22:51.000+00:00';

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", "2024-03-18T22:22:51.000+00:00").load("INSERT PATH HERE")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC ) USING PARQUET
# MAGIC LOCATION 'abfss://demo@formula1dlgio.dfs.core.windows.net/drivers_convert_to_delta'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta 
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_convert_to_delta;

# COMMAND ----------

