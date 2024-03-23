# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

result_races_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(result_races_df)

# COMMAND ----------

demo_df = result_races_df.filter("race_year = 2020")

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

demo_df.groupBy(["race_year"]).agg(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points"), countDistinct("race_name")).withColumnRenamed("sum(points)", "total_points").withColumnRenamed("count(DISTINCT race_name)", "number_of_races").show()

# COMMAND ----------

demo_df.groupBy(["driver_name"]).agg(sum("points")).withColumnRenamed("sum(points)", "total_points").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Window Functions

# COMMAND ----------

demo_window_df = result_races_df.filter("race_year in (2020, 2019)")

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
demo_window_df.withColumn("rank", rank().over())