# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### 6. Ingest Pitstop Json file

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pitstop_schema = StructType(fields = [StructField("raceId",IntegerType(),False),StructField("driverId",IntegerType(),True)\
    ,StructField("stop",IntegerType(),True),StructField("lap",IntegerType(),True),StructField("time",StringType(),True)\
        ,StructField("duration",StringType(),True),StructField("milliseconds",IntegerType(),True)])

# COMMAND ----------

pitstop_df = spark.read.format('json').schema(pitstop_schema)\
    .option('multiLine','true')\
    .load('/mnt/azure_databricks_project_udemy/raw/pit_stops.json')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### STEP 2: Renaming the column names to snake case and addition of ingestion_date.

# COMMAND ----------

pitstop_df = pitstop_df.withColumnRenamed('raceId','race_id')\
    .withColumnRenamed('driverId','driver_id')\
        .withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### STEP 3 : Writing pitstop_df to parquet

# COMMAND ----------

pitstop_df.write.format('parquet').mode('overwrite').save('/mnt/azure_databricks_project_udemy/processed/pit_stops')
