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


df_source_payments = spark.table(f"{BRONZE_CATALOG}.{BRONZE_LAYER}.source_online_payments")

online_payments = df_source_payments.select(
    col("order_id").cast("string").alias("order_id"),
    col("payment_sequential").cast("integer").alias("payment_sequential"),
    col("payment_type").cast("string").alias("payment_type"),
    col("payment_installments").cast("integer").alias("payment_installments"),
    col("payment_value").cast("decimal(14,2)").alias("payment_value"),
).distinct()

online_payments = online_payments\
    .withColumn("payment_label",
                when(col("payment_installments") == 1, "FP")
                .when(col("payment_installments") > 1, "PP")
                .otherwise("OP"))

(online_payments
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(f"{SILVER_CATALOG}.{SILVER_LAYER}.online_payments")
)
