# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## 2. Races csv file ingestion to parquet

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

dbutils.widgets.text("p_date_file","2021-03-21")
w_file_date = dbutils.widgets.get("p_date_file")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### STEP1 : Defining schema's for races csv and reading the races csv file and storing to a df

# COMMAND ----------

from pyspark.sql.types import StructField,StructType,IntegerType,StringType,DateType,TimestampType

schema = StructType(fields=[StructField("raceId",IntegerType(),False),StructField("year",IntegerType(),True),\
    StructField("round",IntegerType(),True),StructField("circuitId",IntegerType(),True),StructField("name",StringType(),True),\
        StructField("date",DateType(),True),StructField("time",StringType(),True),StructField("url",StringType(),True)])

# COMMAND ----------

races_df = spark.read.format('csv').\
    option('header','true').\
        schema(schema).\
            load(f'{mnt_raw_folder_path}/{w_file_date}/races.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### STEP2: selecting the required columns from races_df

# COMMAND ----------

from pyspark.sql.functions import col,concat,date_format,lit

races_df = races_df.select(col('raceId').alias('race_id'),col('year').alias('race_year'),col('round'),col('circuitId').alias('circuit_id')\
    ,col('name'),concat(date_format(col('date'),'yyyy-MM-dd'),lit(" "),col('time')).alias('race_timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### STEP 3: Combining date and time columns and converting string to timestamp using to_timestamp
# MAGIC #### STEP 4: Addition of ingestion_date column to races_df

# COMMAND ----------

from pyspark.sql.functions import col,date_format,lit,to_timestamp,current_timestamp

races_df = races_df.withColumn('race_timestamp',to_timestamp(col('race_timestamp'),'yyyy-MM-dd HH:mm:ss')).\
    withColumn('data_source',lit(data_source))\
        .withColumn('file_date',lit(w_file_date))
races_df = ingestion_date_col_addition(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### STEP 5: Writing final races.csv , partitioningBy 'race_year' df to parquet.

# COMMAND ----------

races_df.write.format('delta').mode('overwrite').partitionBy('race_year').saveAsTable('f1_processed.races')

# COMMAND ----------

dbutils.notebook.exit("Success")
