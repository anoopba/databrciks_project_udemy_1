-- Databricks notebook source
SELECT driver_name ,
       count(1) as total_races,
      sum(calculated_points) as total_points,
      AVG(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year >= 2014
GROUP BY driver_name
HAVING count(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------


