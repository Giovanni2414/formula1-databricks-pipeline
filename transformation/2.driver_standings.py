# Databricks notebook source
# MAGIC %md
# MAGIC #### Produce driver standings

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Find race years for which the data is to be processed

# COMMAND ----------

race_results_list = spark.read.table("f1_presentation.race_results").filter(f"file_date == '{v_file_date}'").select("race_year").distinct().collect()

# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

# COMMAND ----------

race_results_df = spark.read.table("f1_presentation.race_results").filter(col("race_year").isin(race_year_list))

# COMMAND ----------

driver_standings_df = race_results_df.groupBy(["race_year", "driver_name", "driver_nationality"]).agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

# COMMAND ----------

final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

#final_df.write.saveAsTable("f1_presentation.driver_standings", format="delta", mode="overwrite")
#for race_id_list in final_df.select("race_year").distinct().collect():
#    if spark._jsparkSession.catalog().tableExists("f1_presentation.driver_standings"):
#        spark.sql(f"DELETE FROM f1_presentation.driver_standings WHERE race_year = {race_id_list.race_year}")
#        
#final_df.write.saveAsTable("f1_presentation.driver_standings", format="delta", mode="append")
merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta_data(final_df, "f1_presentation", "driver_standings", presentation_folder_path, merge_condition, "race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.driver_standings WHERE race_year = 2021;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, COUNT(1) FROM f1_presentation.driver_standings GROUP BY race_year;

# COMMAND ----------

