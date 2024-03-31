# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### 8. Ingestion of Lap times with a set of csv files

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### STEP 0: Trying to Fetch ingestion_date_col_addition() and variables in configuration_variables

# COMMAND ----------

# MAGIC %run "../child_notebook/configuration_functions"

# COMMAND ----------

# MAGIC %run "../child_notebook/configuration_variables"

# COMMAND ----------

dbutils.widgets.text('data_source_parameter','')
data_source = dbutils.widgets.get('data_source_parameter')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### STEP 1 : Read the folder using spark read api with format csv

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

lap_times_schema = StructType(fields = [StructField("raceId",IntegerType(),True),StructField("driverId",IntegerType(),False)\
    ,StructField("lap",IntegerType(),False),StructField("position",IntegerType(),False),StructField("time",StringType(),False)\
        ,StructField("milliseconds",IntegerType(),False)])

# COMMAND ----------

lap_times = spark.read.format('csv')\
    .schema(lap_times_schema).load('/mnt/azure_databricks_project_udemy/raw/lap_times/lap_times_split*.csv')

# COMMAND ----------

lap_times = lap_times.withColumnRenamed('raceId','race_id').\
    withColumnRenamed('driverId','driver_id').\
        withColumn('data_source',lit(data_source))

# COMMAND ----------

lap_times = ingestion_date_col_addition(lap_times)

# COMMAND ----------

lap_times.write.format('parquet').mode('overwrite').save(f'{mnt_processed_folder_path}/pit_stops')

# COMMAND ----------

dbutils.notebook.exit("Success")
