# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def save_append_to_data_lake(table, final_df):
    for race_id_list in final_df.select("race_id").distinct().collect():
        if spark._jsparkSession.catalog().tableExists(f"{table}"):
            spark.sql(f"DELETE FROM {table} WHERE race_id = {race_id_list.race_id}")

    final_df.write.saveAsTable(f"{table}", format="delta", mode="append")

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
    from delta.tables import DeltaTable

    spark.conf.set("spark.databricks.optimized.dynamicPartitionPruning","true")

    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        deltaTable.alias("tgt").merge(
            input_df.alias("src"), merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")