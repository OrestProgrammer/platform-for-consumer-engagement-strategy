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

from pyspark.sql.functions import col, when


df_source_products = spark.table(f"{BRONZE_CATALOG}.{BRONZE_LAYER}.source_online_products")

online_products = df_source_products.select(
    col("product_id").cast("string").alias("product_id"),
    col("product_category_name").cast("string").alias("product_category_name"),
    col("product_name_lenght").cast("integer").alias("product_name_length"),
    col("product_description_lenght").cast("integer").alias("product_description_length"),
    col("product_photos_qty").cast("integer").alias("product_photos_amount"),
    col("product_weight_g").cast("integer").alias("product_weight_g"),
    col("product_length_cm").cast("integer").alias("product_length_cm"),
    col("product_height_cm").cast("integer").alias("product_height_cm"),
    col("product_width_cm").cast("integer").alias("product_width_cm"),
).distinct()

online_products = online_products.withColumn("product_volume_cm3",
                   col("product_length_cm") * col("product_height_cm") * col("product_width_cm"))

online_products = online_products.withColumn('product_volume_label',
                   when(col('product_volume_cm3') <= 10000, 'SV')
                   .when((col('product_volume_cm3') > 10000) & (col('product_volume_cm3') <= 150000), 'MV')
                   .otherwise('LV'))

(online_products
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(f"{SILVER_CATALOG}.{SILVER_LAYER}.online_products")
)

