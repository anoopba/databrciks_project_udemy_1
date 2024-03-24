# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1.Read the csv file using spark dataframe reader.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### 1.1 Created StructType schema based on Inferschema and reading the circuits csv file.

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

circuits_schema = StructType(fields=[StructField("circuitId",IntegerType(),False),\
    StructField("circuitRef",StringType(),True),StructField("name",StringType(),True),\
        StructField("location",StringType(),True),StructField("country",StringType(),True),\
            StructField("lat",DoubleType(),True),StructField("lng",DoubleType(),True),\
                StructField("alt",IntegerType(),True),StructField("url",StringType(),True)])

# COMMAND ----------

circuits_df = spark.read.format('csv').\
    option("header","true").\
        schema(circuits_schema).\
        load("/mnt/azure_databricks_project_udemy/raw/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### 1.2 Selecting the Required columns for circuits.csv

# COMMAND ----------

from pyspark.sql.functions import col

circuits_df = circuits_df.select(col("circuitId").alias("circuit_id"),col("circuitRef").alias("circuit_ref"),col("name"),col("location"),col("country"),\
    col("lat").alias("latitude"),col("lng").alias("longitude"),col("alt").alias("altitude"))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### 1.3 Addition of new column ingested_date to circuits.csv

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

circuits_final_df = circuits_df.withColumn("ingested_date",current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 1.4 Write the final circuits_final_df to parquet.

# COMMAND ----------

circuits_final_df.write.format("parquet").mode("overwrite").load('/mnt/')

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /mnt
