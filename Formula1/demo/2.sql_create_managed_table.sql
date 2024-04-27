-- Databricks notebook source
-- MAGIC %run "../child_notebook/configuration_variables"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_constructor = spark.read.format('parquet').load(f"{mnt_presentation_folder_path}/constructor_standings")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_constructor.write.format('delta').saveAsTable("demo.constructor_data")

-- COMMAND ----------

select current_database();

-- COMMAND ----------

use demo;

-- COMMAND ----------

drop table constructor_data;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

DESC EXTENDED constructor_data;
