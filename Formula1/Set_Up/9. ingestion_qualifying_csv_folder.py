# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### 8. Ingestion of Qualifying with a set of csv files

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### STEP 1 : Read the folder using spark read api with format csv

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

qualifying_schema = StructType(fields = [StructField("qualifyId",IntegerType(),True),StructField("raceId",IntegerType(),True),StructField("driverId",IntegerType(),False)\
    ,StructField("constructorId",IntegerType(),False),StructField("number",IntegerType(),False),StructField("position",IntegerType(),False),\
        StructField("q1",StringType(),False),StructField("q2",StringType(),False),StructField("q3",StringType(),False)])

# COMMAND ----------

qualifying_df = spark.read.format('json')\
    .schema(qualifying_schema).option('multiLine','true').load('/mnt/azure_databricks_project_udemy/raw/qualifying/')

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### STEP 2: Changing qualifying column names to snake case and adding ingestion_date column

# COMMAND ----------

qualifying_df = qualifying_df.withColumnRenamed('qualifyId','qualify_id').withColumnRenamed('raceId','race_id')\
    .withColumnRenamed('driverId','driver_id').withColumnRenamed('constructorId','constructor_id')

# COMMAND ----------

qualifying_df = qualifying_df.withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### STEP 3: Writing all the Json Qualifying data to parquet

# COMMAND ----------

qualifying_df.write.format('parquet').mode('overwrite').save('/mnt/azure_databricks_project_udemy/processed/qualifying/')

# COMMAND ----------

display(spark.read.parquet('/mnt/azure_databricks_project_udemy/processed/qualifying/'))
