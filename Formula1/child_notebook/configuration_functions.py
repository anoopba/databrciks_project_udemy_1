# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def ingestion_date_col_addition(dataframe):
    dataframe = dataframe.withColumn('ingested_date',current_timestamp())
    return dataframe
