# Databricks notebook source
# MAGIC %md
# MAGIC #Explore DBFS Root
# MAGIC 1. List all the files in DBFS Root.
# MAGIC 2. Interact with DBFS File Browser.
# MAGIC 3. Upload file to DBFS Root

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/'))

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /FileStore/
# MAGIC #dbutils.fs.ls('/FileStore/')

# COMMAND ----------

display(spark.read.option('header','true').csv('/FileStore/circuits.csv'))

# COMMAND ----------

dbutils.fs.ls('')

# COMMAND ----------


