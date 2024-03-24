# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### 3. Ingest Constructor Json file

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Step 1: Read the Json file Using the spark dataframe reader

# COMMAND ----------

constructor_schema = 'constructorId INT,constructorRef STRING,name STRING,nationality STRING,url STRING'

# COMMAND ----------

constructor_df = spark.read.schema(constructor_schema)\
    .json('/mnt/azure_databricks_project_udemy/raw/constructors.json')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### STEP 2 : Dropping url column , renaming specific columns to snake_case and addition of Ingested_Date

# COMMAND ----------

constructor_df = constructor_df.drop('url')

# COMMAND ----------

constructor_df = constructor_df.withColumnRenamed('constructorId','constructor_id').\
    withColumnRenamed('constructorRef','constructor_ref').\
        withColumn('ingested_date',current_timestamp())

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### STEP 3: Writing circuits file to parquet.

# COMMAND ----------

constructor_df.write.format('parquet').mode('overwrite').save('/mnt/azure_databricks_project_udemy/processed/constructors')
