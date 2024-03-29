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
# MAGIC ##### Running configuration_variables and configurations_functions Notebook to access the variables and functions dynamically

# COMMAND ----------

# MAGIC %run "../child_notebook/configuration_variables"

# COMMAND ----------

# MAGIC %run "../child_notebook/configuration_functions"

# COMMAND ----------

mnt_raw_folder_path


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
        load(f"{mnt_raw_folder_path}/circuits.csv")

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
# MAGIC #### 1.3 Addition of new column ingested_date to circuits.csv using ingestion_date_col_addition function from /child_notebook/configuration_functions notebook

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

circuits_final_df = ingestion_date_col_addition(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 1.4 Write the final circuits_final_df to parquet.

# COMMAND ----------

circuits_final_df.write.format("parquet").mode("overwrite").save('/mnt/azure_databricks_project_udemy/processed/circuits')

# COMMAND ----------

display(spark.read.parquet('/mnt/azure_databricks_project_udemy/processed/circuits'))
