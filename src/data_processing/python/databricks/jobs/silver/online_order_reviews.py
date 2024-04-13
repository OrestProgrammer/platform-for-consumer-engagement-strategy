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


df_source_order_reviews = spark.table(f"{BRONZE_CATALOG}.{BRONZE_LAYER}.source_online_order_reviews")

df_source_order_reviews = df_source_order_reviews.filter(~col("review_id").rlike("[ :,\-]"))

online_order_reviews = df_source_order_reviews.select(
    col("review_id").cast("string").alias("review_id"),
    col("order_id").cast("string").alias("order_id"),
    col("review_score").cast("integer").alias("review_score"),
    col("review_comment_title").cast("string").alias("review_comment_title"),
    col("review_comment_message").cast("string").alias("review_comment_message"),
    col("review_creation_date").cast("timestamp").alias("review_creation_timestamp"),
    col("review_answer_timestamp").cast("timestamp").alias("review_answer_timestamp"),
).distinct()

(online_order_reviews
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(f"{SILVER_CATALOG}.{SILVER_LAYER}.online_order_reviews")
)
