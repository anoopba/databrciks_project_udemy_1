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

dbutils.widgets.text("p_file_date","2021-03-21")
w_file_date = dbutils.widgets.get("p_file_date")

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

dbutils.fs.ls('/mnt/azure_databricks_project_udemy/raw/2021-03-21/')

# COMMAND ----------

lap_times = spark.read.format('csv')\
    .schema(lap_times_schema).load(f'{mnt_raw_folder_path}/{w_file_date}/lap_times/lap_times_split*.csv')

# COMMAND ----------

lap_times = lap_times.withColumnRenamed('raceId','race_id').\
    withColumnRenamed('driverId','driver_id').\
        withColumn('data_source',lit(data_source))\
            .withColumn('file_date',lit(w_file_date))

# COMMAND ----------

lap_times = ingestion_date_col_addition(lap_times)

# COMMAND ----------

merge_delta_data(lap_times,'f1_processed','lap_times',"/mnt/azure_databricks_project_udemy/processed/lap_times","targetDF.race_id = input_df.race_id and targetDF.driverId = input_df.driverId and targetDF.lap = input_df.lap","race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")
