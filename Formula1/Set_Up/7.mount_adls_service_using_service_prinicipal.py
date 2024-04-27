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

# display(dbutils.secrets.listScopes())

# COMMAND ----------

# dbutils.secrets.get(scope = "storage_access_key_databricks_ui", key = "ADBClientpasswordorvalue")

# COMMAND ----------

# #1
# service_credential = dbutils.secrets.get(scope = "storage_access_key_databricks_ui", key = "ADBClientpasswordorvalue")
# tenet_id = dbutils.secrets.get(scope = "storage_access_key_databricks_ui", key = "ADBtenetid")
# client_id = dbutils.secrets.get(scope = "storage_access_key_databricks_ui", key = "ADBclientid")

# #2
# spark.conf.set("fs.azure.account.auth.type.anoopdbstorageacc.dfs.core.windows.net", "OAuth")
# spark.conf.set("fs.azure.account.oauth.provider.type.anoopdbstorageacc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set("fs.azure.account.oauth2.client.id.anoopdbstorageacc.dfs.core.windows.net", client_id)
# spark.conf.set("fs.azure.account.oauth2.client.secret.anoopdbstorageacc.dfs.core.windows.net", service_credential)
# spark.conf.set("fs.azure.account.oauth2.client.endpoint.anoopdbstorageacc.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenet_id}/oauth2/token")

# COMMAND ----------

# MAGIC %run "../child_notebook/configuration_variables"

# COMMAND ----------

#Instead of creating the dbutils.fs.mount again and again lets create a function where we can avoid the duplications 
def mount_azure_folders(storage_account_name,container_name):
    service_credential = dbutils.secrets.get(scope = "storage_access_key_databricks_ui", key = "ADBClientpasswordorvalue")
    tenet_id = dbutils.secrets.get(scope = "storage_access_key_databricks_ui", key = "ADBtenetid")
    client_id = dbutils.secrets.get(scope = "storage_access_key_databricks_ui", key = "ADBclientid")

    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": service_credential,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenet_id}/oauth2/token"}
    
    # Optionally, you can add <directory-name> to the source URI of your mount point.
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/azure_databricks_project_udemy/{container_name}",
    extra_configs = configs)

# COMMAND ----------

mount_azure_folders(storage_account_name = f"{storage_account_name}", container_name = "raw")
mount_azure_folders(storage_account_name = f"{storage_account_name}", container_name = "processed")
mount_azure_folders(storage_account_name = f"{storage_account_name}", container_name = "presentation")

# COMMAND ----------

#display(dbutils.fs.mounts())
display(dbutils.fs.ls("/mnt/azure_databricks_project_udemy/"))

# COMMAND ----------

display(dbutils.fs.mounts())
