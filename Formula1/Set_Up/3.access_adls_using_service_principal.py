# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Mount Azure Data Lake Using Service Principal
# MAGIC
# MAGIC ## Steps to follow.
# MAGIC 1. Get client_id,tenet_id and client_server from key_vault.
# MAGIC 2. Set Spark Config with App/Client_id , Directory /  Tenet_id and secret.
# MAGIC 3. Call the file system utility mount (dbutils.fs.mount) to mount.
# MAGIC 4. Explore other file system utilities related to mount (i.e list of all mounts and unmounts)

# COMMAND ----------

display(dbutils.secrets.listScopes())

# COMMAND ----------

dbutils.secrets.get(scope = "storage_access_key_databricks_ui", key = "ADBClientpasswordorvalue")

# COMMAND ----------

#1
service_credential = dbutils.secrets.get(scope = "storage_access_key_databricks_ui", key = "ADBClientpasswordorvalue")
tenet_id = dbutils.secrets.get(scope = "storage_access_key_databricks_ui", key = "ADBtenetid")
client_id = dbutils.secrets.get(scope = "storage_access_key_databricks_ui", key = "ADBclientid")

#2
spark.conf.set("fs.azure.account.auth.type.anoopdbstorageacc.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.anoopdbstorageacc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.anoopdbstorageacc.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.anoopdbstorageacc.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.anoopdbstorageacc.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenet_id}/oauth2/token")

# COMMAND ----------

df = spark.read.format('csv')\
    .option("header","true")\
    .load("abfss://demo@anoopdbstorageacc.dfs.core.windows.net/circuits.csv")

display(df)
