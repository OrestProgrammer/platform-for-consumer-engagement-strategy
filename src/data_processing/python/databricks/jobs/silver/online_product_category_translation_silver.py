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


df_source_product_translations = spark.table(f"{BRONZE_CATALOG}.{BRONZE_LAYER}.source_online_product_category_name_translation")

online_product_category_translation_silver = df_source_product_translations.select(
    col("product_category_name").cast("string").alias("product_category_name_original"),
    col("product_category_name_english").cast("string").alias("product_category_name_english"),
).distinct()

(online_product_category_translation_silver
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(f"{SILVER_CATALOG}.{SILVER_LAYER}.online_product_category_translation_silver")
)
