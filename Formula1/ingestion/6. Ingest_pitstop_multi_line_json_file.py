# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### STEP 0: Trying to Fetch ingestion_date_col_addition() and variables in configuration_variables

# COMMAND ----------

# MAGIC %run "../child_notebook/configuration_functions"

# COMMAND ----------

# MAGIC %run "../child_notebook/configuration_variables"

# COMMAND ----------

dbutils.widgets.text("data_source_parameter","")
data_source = dbutils.widgets.get("data_source_parameter")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
w_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 6. Ingest Pitstop Json file

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

pitstop_schema = StructType(fields = [StructField("raceId",IntegerType(),False),StructField("driverId",IntegerType(),True)\
    ,StructField("stop",IntegerType(),True),StructField("lap",IntegerType(),True),StructField("time",StringType(),True)\
        ,StructField("duration",StringType(),True),StructField("milliseconds",IntegerType(),True)])

# COMMAND ----------

pitstop_df = spark.read.format('json').schema(pitstop_schema)\
    .option('multiLine','true')\
    .load(f'{mnt_raw_folder_path}/{w_file_date}/pit_stops.json')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### STEP 2: Renaming the column names to snake case and addition of ingestion_date.

# COMMAND ----------

pitstop_df = pitstop_df.withColumnRenamed('raceId','race_id')\
    .withColumnRenamed('driverId','driver_id').\
        withColumn('data_source',lit(data_source))\
            .withColumn('file_date',lit(w_file_date))

# COMMAND ----------

pitstop_df = ingestion_date_col_addition(pitstop_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### STEP 3 : Writing pitstop_df to parquet

# COMMAND ----------

merge_delta_data(pitstop_df,'f1_processed','pit_stops',"/mnt/azure_databricks_project_udemy/processed/pit_stops","targetDF.race_id = input_df.race_id and targetDF.driver_id = input_df.driver_id and targetDF.stop = input_df.stop","race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")
