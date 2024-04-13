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
from pyspark.sql.functions import year, month, dayofmonth

    
df_source_orders = spark.table(f"{BRONZE_CATALOG}.{BRONZE_LAYER}.source_online_order")

online_orders = df_source_orders.select(
    col("order_id").cast("string").alias("order_id"),
    col("customer_unique_id").cast("string").alias("consumer_unique_id"),
    col("order_status").cast("string").alias("order_status"),
    col("order_purchase_timestamp").cast("timestamp").alias("order_purchase_timestamp"),
    col("order_approved_at").cast("timestamp").alias("order_approved_at_timestamp"),
    col("order_delivered_carrier_date").cast("timestamp").alias("order_delivered_carrier_timestamp"),
    col("order_delivered_customer_date").cast("timestamp").alias("order_delivered_customer_timestamp"),
    col("order_estimated_delivery_date").cast("date").alias("order_estimated_delivery_date"),
).distinct()

online_orders = online_orders.withColumn("order_purchase_year", year(online_orders["order_purchase_timestamp"]))
online_orders = online_orders.withColumn("order_purchase_month", month(online_orders["order_purchase_timestamp"]))
online_orders = online_orders.withColumn("order_purchase_day", dayofmonth(online_orders["order_purchase_timestamp"]))

online_orders = online_orders\
    .withColumn("order_quality_label",
                when(col("order_status") == "canceled", "CO")
                .otherwise("GO"))

(online_orders
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(f"{SILVER_CATALOG}.{SILVER_LAYER}.online_order")
)
