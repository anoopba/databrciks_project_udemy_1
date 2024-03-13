# Databricks notebook source
# MAGIC %md
# MAGIC **Access Azure Data Lake Using access Keys**
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

dbutils.fs.ls("abfss://@")
