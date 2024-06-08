# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### 3. Ingest Constructor Json file

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

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

dbutils.widgets.text("p_file_date","")
w_date_file = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 1: Read the Json file Using the spark dataframe reader

# COMMAND ----------

constructor_schema = 'constructorId INT,constructorRef STRING,name STRING,nationality STRING,url STRING'

# COMMAND ----------

constructor_df = spark.read.schema(constructor_schema)\
    .json(f'{mnt_raw_folder_path}/{w_date_file}/constructors.json')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### STEP 2 : Dropping url column , renaming specific columns to snake_case and addition of Ingested_Date

# COMMAND ----------

constructor_df = constructor_df.drop('url')

# COMMAND ----------

constructor_df = constructor_df.withColumnRenamed('constructorId','constructor_id').\
    withColumnRenamed('constructorRef','constructor_ref').\
        withColumn('data_source',lit(data_source))\
            .withColumn('file_date',lit(w_date_file))

# COMMAND ----------

constructor_df = ingestion_date_col_addition(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### STEP 3: Writing circuits file to parquet.

# COMMAND ----------

constructor_df.write.format('delta').mode('overwrite').saveAsTable('f1_processed.constructors')

# COMMAND ----------

dbutils.notebook.exit("Sucess")
