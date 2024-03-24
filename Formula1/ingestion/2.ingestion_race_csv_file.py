# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## 2. Races csv file ingestion to parquet

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
            load('/mnt/azure_databricks_project_udemy/raw/races.csv')

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

races_df = races_df.withColumn('race_timestamp',to_timestamp(col('race_timestamp'),'yyyy-MM-dd HH:mm:ss'))
races_df = races_df.withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### STEP 5: Writing final races.csv , partitioningBy 'race_year' df to parquet.

# COMMAND ----------

races_df.write.format('parquet').mode('overwrite').partitionBy('race_year').save('/mnt/azure_databricks_project_udemy/processed/races')
