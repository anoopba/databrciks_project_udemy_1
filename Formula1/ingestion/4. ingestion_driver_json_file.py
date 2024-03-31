# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## 4. driver Json File Ingestion

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

# MAGIC %md
# MAGIC
# MAGIC #### STEP 1: In driver Json file we have a nested data so we are creating two structfields.

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import concat,col,current_timestamp,lit

# COMMAND ----------

names_schema = StructType(fields = [StructField("forename",StringType(),True),StructField("surname",StringType(),True)])

# COMMAND ----------

driver_schema = StructType(fields = [StructField("driverId",IntegerType(),False),StructField("driverRef",StringType(),True),\
    StructField("number",IntegerType(),True),StructField("code",StringType(),True),\
        StructField("name",names_schema),StructField("dob",StringType(),True)])

# COMMAND ----------

drivers_df = spark.read.schema(driver_schema).\
    json(f'{mnt_raw_folder_path}/drivers.json')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### STEP 2: Rename Columns and Add New Columns
# MAGIC 1. driverId to driver_id
# MAGIC 2. driverRef to driver_ref
# MAGIC 3. Ingested date column addition
# MAGIC 4. concat forenmae and surname and rename it as 

# COMMAND ----------

drivers_df = drivers_df.withColumnRenamed('driverId','driver_id').\
    withColumnRenamed('driverRef','driver_ref').\
        withColumn('name',concat(col('name.forename'),lit(" "),col('name.surname'))).\
            withColumn('data_source_parameter',lit(data_source))

# COMMAND ----------

drivers_df = ingestion_date_col_addition(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### STEP 3 : drop name.forename , name.surname

# COMMAND ----------

drivers_df = drivers_df.drop('name.forename','name.surname')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### STEP 4 : Write driver.json df data to parquet to processed folder

# COMMAND ----------

drivers_df.write.format('parquet').mode('overwrite').save(f'{mnt_processed_folder_path}/drivers')

# COMMAND ----------

dbutils.notebook.exit("Success")
