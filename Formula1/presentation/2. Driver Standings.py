# Databricks notebook source
# MAGIC %md
# MAGIC #### Driver Standings

# COMMAND ----------

# MAGIC %run "../child_notebook/configuration_variables"

# COMMAND ----------

# MAGIC %run "../child_notebook/configuration_functions"

# COMMAND ----------

from pyspark.sql.functions import col,when,sum,count,lit,rank,desc
from pyspark.sql.window import Window


# COMMAND ----------

final_result_df = spark.read.format('delta').load(f"{mnt_presentation_folder_path}/race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. For Driver standings we have the above data.

# COMMAND ----------


driver_standings_df = final_result_df.groupBy("race_year","driver_name","driver_nationality")\
    .agg(sum("points").alias("total_points"),\
        count(when(col('position') == 1,True)).alias('wins'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Ranking based on highest number of points and wins for the particular year
# MAGIC #### Why order by total points and wins because if the total points are tied then based on wins the data will be ordered

# COMMAND ----------

window_spec = Window.partitionBy('race_year').orderBy(desc('total_points'),desc('wins'))
driver_standings_df = driver_standings_df.withColumn('rank',rank().over(window_spec))

# COMMAND ----------

merge_delta_data(driver_standings_df,'f1_presentation','driver_standings',f"{mnt_presentation_folder_path}/driver_standings","targetDF.driver_name = input_df.driver_name and targetDF.race_year = input_df.race_year","race_year")
