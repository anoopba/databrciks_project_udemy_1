# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Read the Parquet files required to join all the tables
# MAGIC 1. races
# MAGIC 2. circuits
# MAGIC 3. drivers
# MAGIC 4. constructors
# MAGIC 5. results

# COMMAND ----------

# MAGIC %run "../child_notebook/configuration_variables"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

races_df = spark.read.parquet('/mnt/azure_databricks_project_udemy/processed/races/')\
    .withColumnRenamed('name','race_name')\
        .withColumnRenamed('race_timestamp','race_date')

# COMMAND ----------

circuits_df = spark.read.parquet('/mnt/azure_databricks_project_udemy/processed/circuits/')\
    .withColumnRenamed('location','circuit_location')

# COMMAND ----------

drivers_df = spark.read.parquet('/mnt/azure_databricks_project_udemy/processed/drivers/')\
    .withColumnRenamed('name','driver_name')\
        .withColumnRenamed('number','driver_number')\
            .withColumnRenamed('nationality','driver_nationality')

# COMMAND ----------

constructors_df = spark.read.parquet('/mnt/azure_databricks_project_udemy/processed/constructors/')\
    .withColumnRenamed('name','team')

# COMMAND ----------

results_df = spark.read.parquet('/mnt/azure_databricks_project_udemy/processed/results/')\
    .withColumnRenamed('time','race_time')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 2. Joining races data to fetch circuit_location column from circuits id

# COMMAND ----------

# MAGIC %md
# MAGIC - First we wanted circuit location so mapped with races and circuits

# COMMAND ----------

races_circuits_join = races_df.join(circuits_df,(races_df['circuit_id'] == circuits_df['circuit_id']),how = 'inner').\
    select(races_df['race_id'],races_df['race_name'],races_df['race_date'],races_df['race_year'],circuits_df['circuit_location'])

# COMMAND ----------

final_df = results_df.join(races_circuits_join,(results_df['race_id'] == races_circuits_join['race_id']),'inner')\
    .join(drivers_df,(results_df['driver_id'] == drivers_df['driver_id']),'inner')\
        .join(constructors_df,(results_df['constructor_id'] == constructors_df['constructor_id']),'inner')


# COMMAND ----------

final_df = final_df.select("race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","position").\
    withColumn('created_date',current_timestamp())

# COMMAND ----------

final_df.write.mode('overwrite').format('parquet').saveAsTable("f1_presentation.race_results")

# COMMAND ----------

display(spark.read.parquet(f"{mnt_presentation_folder_path}/race_results"))
