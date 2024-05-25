-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### 1. Lets create a database f1_raw 

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2. Creating External Tables for Raw data or files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING CSV
OPTIONS (path "/mnt/azure_databricks_project_udemy/raw/circuits.csv", header true)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING
)
USING CSV
OPTIONS (path "/mnt/azure_databricks_project_udemy/raw/races.csv", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3. Create Tables for Json files.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Constructor table
-- MAGIC - Single Line Json
-- MAGIC - Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructor;
CREATE TABLE IF NOT EXISTS f1_raw.constructor (
  constructorId INT,
  constructorRef STRING,
  name STRING,nationality STRING,
  url STRING
)
USING JSON
OPTIONS (path "/mnt/azure_databricks_project_udemy/raw/constructors.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Drivers table
-- MAGIC - Single Line Json
-- MAGIC - Complex Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
name STRUCT<forename : STRING, surname : STRING>,
dob DATE,
nationality STRING,
url STRING
)
USING JSON
OPTIONS (path "/mnt/azure_databricks_project_udemy/raw/drivers.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Results Table
-- MAGIC - Single Line Json
-- MAGIC - Simple Structure

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT,
position INT,
positionText INT,
positionOrder INT,
points INT,
laps INT,
time INT,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId INT
)
USING JSON
OPTIONS (PATH "/mnt/azure_databricks_project_udemy/raw/results.json")


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Pit-stops table
-- MAGIC - Multi Line Json
-- MAGIC - Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stop;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stop(
raceId INT,
driverId INT,
stop INT,
lap INT,
time STRING,
duration STRING,
milliseconds INT
)
USING JSON
OPTIONS (PATH "/mnt/azure_databricks_project_udemy/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Creating Tables for List of Files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Creating Lap Times Tables
-- MAGIC - csv files
-- MAGIC - Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING CSV
OPTIONS (PATH "/mnt/azure_databricks_project_udemy/raw/lap_times")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Creating qualifying Tables
-- MAGIC - Json files
-- MAGIC - MultiLine Jsons
-- MAGIC - Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
qualifyId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING
)
USING JSON
OPTIONS (path "/mnt/azure_databricks_project_udemy/raw/qualifying", multiLine true)
