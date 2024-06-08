# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### 8. Ingestion of Qualifying with a set of csv files

# COMMAND ----------

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
# MAGIC #### STEP 1 : Read the folder using spark read api with format csv

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

qualifying_schema = StructType(fields = [StructField("qualifyId",IntegerType(),True),StructField("raceId",IntegerType(),True),StructField("driverId",IntegerType(),False)\
    ,StructField("constructorId",IntegerType(),False),StructField("number",IntegerType(),False),StructField("position",IntegerType(),False),\
        StructField("q1",StringType(),False),StructField("q2",StringType(),False),StructField("q3",StringType(),False)])

# COMMAND ----------

qualifying_df = spark.read.format('json')\
    .schema(qualifying_schema).option('multiLine','true').load(f'{mnt_raw_folder_path}/{w_file_date}/qualifying/')

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### STEP 2: Changing qualifying column names to snake case and adding ingestion_date column

# COMMAND ----------

qualifying_df = qualifying_df.withColumnRenamed('qualifyId','qualify_id').withColumnRenamed('raceId','race_id')\
    .withColumnRenamed('driverId','driver_id').withColumnRenamed('constructorId','constructor_id').\
        withColumn("data_source",lit("data_source"))\
            .withColumn('file_date',lit(w_file_date))

# COMMAND ----------

qualifying_df = qualifying_df.withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### STEP 3: Writing all the Json Qualifying data to parquet

# COMMAND ----------

merge_delta_data(qualifying_df,'f1_processed','qualifying',"/mnt/azure_databricks_project_udemy/processed/qualifying","targetDF.qualify_id = input_df.qualify_id","race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")
