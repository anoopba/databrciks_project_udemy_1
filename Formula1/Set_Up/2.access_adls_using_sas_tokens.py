# Databricks notebook source
# MAGIC %md
# MAGIC **Access Azure Data Lake Using  Keys**
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.anoopstoragetry.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.anoopstoragetry.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
# spark.conf.set("fs.azure.sas.fixed.token.anoopstoragetry.dfs.core.windows.net", dbutils.secrets.get(scope="<scope>", key="<sas-token-key>"))
#In  the above they are getting the data from secrets.
spark.conf.set("fs.azure.sas.fixed.token.anoopstoragetry.dfs.core.windows.net", "sp=rl&st=2024-03-15T12:32:22Z&se=2024-03-15T20:32:22Z&sv=2022-11-02&sr=c&sig=SDq%2FPOZ88WrlTZGdbrPxUAWYPD5gdbSPo%2BjD66WijG8%3D")

# COMMAND ----------

#Below we can see only circuits file is there so circuits file is displayed.
dbutils.fs.ls("abfss://demo@anoopstoragetry.dfs.core.windows.net")

# COMMAND ----------

#for better visualizations.
# fs.ls --> display is converting the fileInfo path 
display(dbutils.fs.ls("abfss://demo@anoopstoragetry.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.format('csv').option('header','true').load('abfss://demo@anoopstoragetry.dfs.core.windows.net/circuits.csv'))
