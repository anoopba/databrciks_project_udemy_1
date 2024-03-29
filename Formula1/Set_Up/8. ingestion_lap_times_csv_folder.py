# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### 8. Ingestion of Lap times with a set of csv files

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### STEP 1 : Read the folder using spark read api with format csv

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_times_schema = StructType(fields = [StructField("raceId",IntegerType(),True),StructField("driverId",IntegerType(),False)\
    ,StructField("lap",IntegerType(),False),StructField("position",IntegerType(),False),StructField("time",StringType(),False)\
        ,StructField("milliseconds",IntegerType(),False)])

# COMMAND ----------

lap_times = spark.read.format('csv')\
    .schema(lap_times_schema).load('/mnt/azure_databricks_project_udemy/raw/lap_times/lap_times_split*.csv')

# COMMAND ----------

display(lap_times.count())

# COMMAND ----------

display(dbutils.fs.ls('/mnt/azure_databricks_project_udemy/raw/lap_times/'))

# COMMAND ----------


