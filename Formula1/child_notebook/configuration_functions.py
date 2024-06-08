# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def ingestion_date_col_addition(dataframe):
    dataframe = dataframe.withColumn('ingested_date',current_timestamp())
    return dataframe

# COMMAND ----------

def re_arrange_partition_column(input_df,partition_column):
    input_df = input_df.schema.names
    columns = []
    for column_name in input_df:
        if column_name != partition_column:
            columns.append(column_name)
        else:
            columns.append(partition_column)
    output_df = input_df.select(columns)
    return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
  output_df = re_arrange_partition_column(input_df, partition_column)
  spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
  else:
    output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

from delta.tables import DeltaTable

spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):

    targetDF = DeltaTable.forPath(spark,folder_path)
    targetDF.alias('targetDF')\
    .merge(input_df.alias('input_df'), merge_condition)\
      .whenMatchedUpdateAll()\
      .whenNotMatchedInsertAll()\
      .execute()
  else : 
      input_df.write.format('delta').mode('overwrite').partitionBy(partition_column).saveAsTable(f'{db_name}.{table_name}')
