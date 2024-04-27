# Databricks notebook source
# MAGIC %md
# MAGIC #### Constructor Standings

# COMMAND ----------

# MAGIC %run "../child_notebook/configuration_variables"

# COMMAND ----------

from pyspark.sql.functions import col,when,sum,count,lit,rank,desc
from pyspark.sql.window import Window


# COMMAND ----------

final_result_df = spark.read.parquet(f"{mnt_presentation_folder_path}/race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. For Driver standings we have the above data.

# COMMAND ----------


driver_standings_df = final_result_df.groupBy("race_year","team")\
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

driver_standings_df.write.mode('overwrite').parquet(f"{mnt_presentation_folder_path}/constructor_standings")
