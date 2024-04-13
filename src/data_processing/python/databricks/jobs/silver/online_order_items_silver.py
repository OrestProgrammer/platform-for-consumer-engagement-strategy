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


df_source_order_items = spark.table(f"{BRONZE_CATALOG}.{BRONZE_LAYER}.source_online_order_items")

online_order_items = df_source_order_items.select(
    col("order_id").cast("string").alias("order_id"),
    col("order_item_id").cast("integer").alias("order_item_id"),
    col("product_id").cast("string").alias("product_id"),
    col("shipping_limit_date").cast("timestamp").alias("shipping_limit_timestamp"),
    col("price").cast("decimal(14,2)").alias("item_price"),
    col("freight_value").cast("decimal(14,2)").alias("item_freight_value"),
).distinct()

online_order_items = online_order_items.withColumn("item_price_category_label",
                   when(col("item_price") <= 50, "CI")
                   .when((col("item_price") > 50) & (col("item_price") <= 200), "MI")
                   .otherwise("EI"))


(online_order_items
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(f"{SILVER_CATALOG}.{SILVER_LAYER}.online_order_items")
)
