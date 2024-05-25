# Databricks notebook source
dbutils.notebook.run("1. ingestion_circuits_csv_file",0,arguments={"data_source_parameter":"ERGAST API","p_file_date":"2021-03-21"})

# COMMAND ----------

dbutils.notebook.run("2.ingestion_race_csv_file",0,arguments={"data_source_parameter":"ERGAST API","p_file_date":"2021-03-21"})

# COMMAND ----------

dbutils.notebook.run("3.ingestion_constructor_json_file",0,arguments={"data_source_parameter":"ERGAST API","p_file_date":"2021-03-21"})

# COMMAND ----------

dbutils.notebook.run("4. ingestion_driver_json_file",0,arguments={"data_source_parameter":"ERGAST API","p_file_date":"2021-03-21"})

# COMMAND ----------

dbutils.notebook.run("5.ingestion_result_file",0,arguments={"data_source_parameter":"ERGAST API","p_file_date":"2021-03-21"})

# COMMAND ----------

dbutils.notebook.run("6. Ingest_pitstop_multi_line_json_file",0,arguments={"data_source_parameter":"ERGAST API","p_file_date":"2021-03-21"})

# COMMAND ----------

dbutils.notebook.run("7. ingestion_lap_times_csv_folder",0,arguments={"data_source_parameter":"ERGAST API","p_file_date":"2021-03-21"})

# COMMAND ----------

dbutils.notebook.run("8. ingestion_qualifying_csv_folder",0,arguments={"data_source_parameter":"ERGAST API","p_file_date":"2021-03-21"})
