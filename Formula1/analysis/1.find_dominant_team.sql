-- Databricks notebook source
SELECT team_name ,
       count(1) as total_races,
      sum(calculated_points) as total_points,
      AVG(calculated_points) as avg_points
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING count(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------


