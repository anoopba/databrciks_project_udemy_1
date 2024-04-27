-- Databricks notebook source
-- MAGIC %run "../child_notebook/configuration_variables"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_constructor = spark.read.format('parquet').load(f"{mnt_presentation_folder_path}/constructor_standings")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_constructor.write.format("parquet").mode('overwrite').option("path",f"{mnt_presentation_folder_path}/constructor_ext_py").saveAsTable("demo.constructor_ext_py")

-- COMMAND ----------

CREATE TABLE demo.constructor_standings_ext_sql
(race_year INTEGER,
team STRING,
total_points INTEGER,
wins INTEGER,
rank INTEGER
)
USING PARQUET
LOCATION "/mnt/azure_databricks_project_udemy/presentation/constructor_standings_ext_sql"

-- COMMAND ----------

DROP table demo.constructor_standings_ext_sql

-- COMMAND ----------

INSERT INTO TABLE demo.constructor_standings_ext_sql 
select * from demo.constructor_ext_py;

-- COMMAND ----------

DROP TABLE demo.constructor_standings_ext_sql;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------


