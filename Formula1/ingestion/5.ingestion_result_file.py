# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## 5. Result json file ingestion

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
# MAGIC #### STEP 1 : Creating schema and Reading Result json file fom sparkreader api

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,StringType,IntegerType,FloatType
from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

#JSON document - {"resultId":1,"raceId":18,"driverId":1,"constructorId":1,"number":22,"grid":1,"position":1,"positionText":1,"positionOrder":1,"points":10,"laps":58,"time":"1:34:50.616","milliseconds":5690616,"fastestLap":39,"rank":2,"fastestLapTime":"1:27.452","fastestLapSpeed":218.3,"statusId":1}

results_schema = StructType(fields = [StructField("resultId",IntegerType(),False),StructField("raceId",IntegerType(),True),StructField("driverId",IntegerType(),True),\
    StructField("constructorId",IntegerType(),True),StructField("number",IntegerType(),True),StructField("grid",IntegerType(),True),StructField("position",IntegerType(),True),\
        StructField("positionText",IntegerType(),True),StructField("positionOrder",IntegerType(),True),StructField("points",IntegerType(),True),\
            StructField("laps",IntegerType(),True),StructField("time",StringType(),True),StructField("milliseconds",IntegerType(),True),\
                StructField("fastestLap",IntegerType(),True),StructField("rank",IntegerType(),True),StructField("fastestLapTime",StringType(),True),\
                    StructField("fastestLapSpeed",FloatType(),True),StructField("statusId",IntegerType(),True)])

# COMMAND ----------

result_df = spark.read.format('json').\
    schema(results_schema).\
        load(f'{mnt_raw_folder_path}/results.json')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### STEP 2 : Dropping URL column

# COMMAND ----------

result_df = result_df.drop('statusId')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### converting the columns names to snake case and addition of ingestion_

# COMMAND ----------

##{"resultId":1,"raceId":18,"driverId":1,"constructorId":1,"number":22,"grid":1,"position":1,"positionText":1,"positionOrder":1,"points":10,"laps":58,"time":"1:34:50.616","milliseconds":5690616,"fastestLap":39,"rank":2,"fastestLapTime":"1:27.452","fastestLapSpeed":218.3,"statusId":1}

result_df = result_df.withColumnRenamed('resultId','result_id').withColumnRenamed('raceId','race_id')\
    .withColumnRenamed('driverId','driver_id').withColumnRenamed('constructorId','constructor_id')\
        .withColumnRenamed('positionText','position_text').withColumnRenamed('positionOrder','position_order')\
            .withColumnRenamed('fastestLap','fastest_lap').withColumnRenamed('fastestLapTime','fastest_lap_time')\
                .withColumnRenamed('fastestLapSpeed','fastest_lap_speed').withColumnRenamed('statusId','status_id').\
                    withColumn('data_source',lit(data_source))

# COMMAND ----------

result_df = ingestion_date_col_addition(result_df)

# COMMAND ----------

result_df.write.format('parquet').mode('overwrite').partitionBy('race_id').save(f'{mnt_processed_folder_path}/results')

# COMMAND ----------

dbutils.notebook.exit("Success")
