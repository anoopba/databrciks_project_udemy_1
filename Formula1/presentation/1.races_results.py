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

# MAGIC %run "../child_notebook/configuration_functions"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
w_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

races_df = spark.read.format('delta')\
    .load(f'{mnt_processed_folder_path}/races/')\
    .withColumnRenamed('name','race_name')\
        .withColumnRenamed('race_timestamp','race_date')
            

# COMMAND ----------

circuits_df = spark.read.format('delta')\
        .load(f'{mnt_processed_folder_path}/circuits/')\
            .withColumnRenamed('location','circuit_location')
        

# COMMAND ----------

drivers_df = spark.read.format('delta')\
    .load(f'{mnt_processed_folder_path}/drivers/')\
    .withColumnRenamed('name','driver_name')\
        .withColumnRenamed('number','driver_number')\
            .withColumnRenamed('nationality','driver_nationality')
                

# COMMAND ----------

constructors_df = spark.read.format('delta')\
    .load(f'{mnt_processed_folder_path}/constructors/')\
    .withColumnRenamed('name','team')
        

# COMMAND ----------

results_df = spark.read.format('delta')\
    .load(f'{mnt_processed_folder_path}/results/')\
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

races_circuits_join = races_circuits_join.withColumnRenamed('race_id','races_circuits_race_id')

# COMMAND ----------

final_df = results_df.join(races_circuits_join,(results_df['race_id'] == races_circuits_join['races_circuits_race_id']),'inner')\
    .join(drivers_df,(results_df['driver_id'] == drivers_df['driver_id']),'inner')\
        .join(constructors_df,(results_df['constructor_id'] == constructors_df['constructor_id']),'inner')


# COMMAND ----------

final_df = final_df.select('race_id',"race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","position").\
    withColumn('created_date',current_timestamp())\
        .withColumn('result_file_date',lit(w_file_date))

# COMMAND ----------

merge_delta_data(final_df,'f1_presentation','race_results',f"{mnt_presentation_folder_path}/race_results","targetDF.driver_name = input_df.driver_name and targetDF.race_id = input_df.race_id","race_id")

# COMMAND ----------

display(spark.read.format('delta').load(f"{mnt_presentation_folder_path}/race_results"))
