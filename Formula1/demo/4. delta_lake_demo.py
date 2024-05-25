# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to Delta Lake (Managed Table)
# MAGIC 2. Write data to Delta Lake (External Table)
# MAGIC 3. Read data from Delta Lake (Table)
# MAGIC 4. Read data from Delta Lake (File)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql 
# MAGIC show databases;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION "/mnt/azure_databricks_project_udemy/demo";

# COMMAND ----------

df = spark.read.format('json')\
    .load('/mnt/azure_databricks_project_udemy/raw/2021-03-28/constructors.json')

# COMMAND ----------

display(df.count())

# COMMAND ----------

df.write.format('delta').mode('overwrite').save('/mnt/azure_databricks_project_udemy/raw/constructors_delta_data')

# COMMAND ----------

from delta.tables import DeltaTable

delta_table_name = DeltaTable.forName(spark,'f1_demo.external_table_new')
delta_table_path = DeltaTable.forPath(spark,'/mnt/azure_databricks_project_udemy/raw/constructors_delta_data')

# COMMAND ----------

from pyspark.sql.functions import  *

df = spark.read.format('delta').load('/mnt/azure_databricks_project_udemy/raw/constructors_delta_data')
df.filter(col('name') == 'MF1').show()

# COMMAND ----------

display(spark.sql(''' select * from f1_demo.external_table_new where name = 'MF1' '''))

# COMMAND ----------

delta_table_name.update(
    set = {"constructorId": when(col('name') != 'Ferrari',1012).\
        otherwise('constructorId') }, 
    condition= col('name') == 'MF1')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.external_table_new
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/azure_databricks_project_udemy/raw/constructors_delta_data'

# COMMAND ----------

display(spark.read.format('delta').load('/mnt/azure_databricks_project_udemy/raw/constructors_delta_data'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_demo.external_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.external_table
# MAGIC SET constructorRef = CASE 
# MAGIC WHEN name = 'Ferrari' then 'F'
# MAGIC WHEN name = 'Toyota' then 'T'
# MAGIC ELSE constructorRef
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.external_table
# MAGIC SET constructorRef = 'anoop' , nationality = 'BRAZIL'
# MAGIC WHERE name = 'MF1'

# COMMAND ----------

# MAGIC %md
# MAGIC 1. MERGE

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

df_day_1 = spark.read\
    .option("inferSchema",True)\
    .json("/mnt/azure_databricks_project_udemy/raw/2021-03-28/drivers.json")\
    .filter(col('driverId') <= 10)\
    .select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

display(df_day_1)

# COMMAND ----------

df_day_1.createOrReplaceTempView('df_day_1')

# COMMAND ----------

df_day_2 = spark.read\
    .option("inferSchema",True)\
    .json("/mnt/azure_databricks_project_udemy/raw/2021-03-28/drivers.json")\
    .filter(col('driverId').between(6,15))\
    .select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

display(df_day_2)

# COMMAND ----------

df_day_2.createOrReplaceTempView('df_day_2')

# COMMAND ----------

df_day_3 = spark.read\
    .option("inferSchema",True)\
    .json("/mnt/azure_databricks_project_udemy/raw/2021-03-28/drivers.json")\
    .filter('driverId between 1 and 5 OR driverId between 16 and 20')\
    .select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

display(df_day_3)

# COMMAND ----------

df_day_3.createOrReplaceTempView('df_day_3')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge_data (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge_data as tgt
# MAGIC USING df_day_1 as upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC               tgt.forename = upd.forename,
# MAGIC               tgt.surname = upd.surname,
# MAGIC               tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename,surname,createdDate) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from f1_demo.drivers_merge_data

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge_data as tgt
# MAGIC USING df_day_2 as upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC               tgt.forename = upd.forename,
# MAGIC               tgt.surname = upd.surname,
# MAGIC               tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename,surname,createdDate) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forName(spark,'f1_demo.drivers_merge_data')

deltaTable.alias('tgt') \
  .merge(
    df_day_3.alias('updates'),
    'tgt.driverId = updates.driverId and tgt.driverId = updates.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
      "dob": "updates.dob",
      "forename": "updates.forename",
      "surname": "updates.surname",
      "updatedDate": "current_timestamp",
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "updates.driverId",
      "dob": "updates.dob",
      "forename": "updates.forename",
      "surname": "updates.surname",
      "createdDate": "current_timestamp()",
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge_data VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge_data TIMESTAMP AS OF "2024-05-23T05:45:08.000+00:00"

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC FORMATTED f1_demo.drivers_merge_data

# COMMAND ----------

df = spark.read.format('delta').option('timestampAsOf','2024-05-23T05:45:08.000+00:00').load('dbfs:/mnt/azure_databricks_project_udemy/demo/drivers_merge_data')

# COMMAND ----------

display(df)

# COMMAND ----------

df = spark.read.format('delta').option('versionAsOf','1').load('dbfs:/mnt/azure_databricks_project_udemy/demo/drivers_merge_data')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merge_data RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM f1_demo.drivers_merge_data ;
# MAGIC --VERSION AS OF 5;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC FORMATTED f1_demo.drivers_merge;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_txn 
# MAGIC WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  * FROM f1_demo.drivers_txn ;

# COMMAND ----------

for driverId in range(3,20):
    spark.sql(f''' INSERT INTO f1_demo.drivers_txn
SELECT * FROM f1_demo.drivers_merge
WHERE driverId = {driverId} ''')
