-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Learning Objectives
-- MAGIC 1. Create Database Demo
-- MAGIC 2. Data tab in the UI
-- MAGIC 3. SHOW Command
-- MAGIC 4. Describe Command
-- MAGIC 5. Find the current Database

-- COMMAND ----------

CREATE DATABASE demo;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESC DATABASE demo;

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

show tables;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

USE DATABASE demo;

-- COMMAND ----------

select current_database();
