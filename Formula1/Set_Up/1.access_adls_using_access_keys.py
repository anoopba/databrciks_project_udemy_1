# Databricks notebook source
# MAGIC %md
# MAGIC **Access Azure Data Lake Using access Keys**
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = "storage_access_key_databricks_ui")

# COMMAND ----------

access_key = dbutils.secrets.get(scope = "storage_access_key_databricks_ui",key = "storageaccesskey")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.anoopdbstorageacc.dfs.core.windows.net",
    dbutils.secrets.get(scope="storage_access_key_databricks_ui", key="storageaccesskey"))

# COMMAND ----------

df = spark.read.format('csv')\
    .option("header","true")\
    .load("abfss://demo@anoopdbstorageacc.dfs.core.windows.net/circuits.csv")

display(df)
