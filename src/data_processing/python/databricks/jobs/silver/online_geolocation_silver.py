# Databricks notebook source
dbutils.widgets.text("BRONZE_CATALOG", "")
dbutils.widgets.text("BRONZE_LAYER", "")
dbutils.widgets.text("SILVER_CATALOG", "")
dbutils.widgets.text("SILVER_LAYER", "")

# COMMAND ----------

BRONZE_CATALOG = dbutils.widgets.get("BRONZE_CATALOG")
BRONZE_LAYER = dbutils.widgets.get("BRONZE_LAYER")
SILVER_CATALOG = dbutils.widgets.get("SILVER_CATALOG")
SILVER_LAYER = dbutils.widgets.get("SILVER_LAYER")

# COMMAND ----------

from pyspark.sql.functions import col


df_source_geolocation = spark.table(f"{BRONZE_CATALOG}.{BRONZE_LAYER}.source_online_geolocation")

online_geolocation = df_source_geolocation.select(
    col("geolocation_zip_code_prefix").cast("string").alias("geolocation_zip_code"),
    col("geolocation_lat").cast("double").alias("geolocation_latitude"),
    col("geolocation_lng").cast("double").alias("geolocation_longitude"),
    col("geolocation_city").cast("string").alias("geolocation_city"),
    col("geolocation_state").cast("string").alias("geolocation_state"),
).distinct()

(online_geolocation
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(f"{SILVER_CATALOG}.{SILVER_LAYER}.online_geolocation")
)
