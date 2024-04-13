# Databricks notebook source
dbutils.widgets.text("INGESTION_PATH", "")
dbutils.widgets.text("BRONZE_LAYER", "")
dbutils.widgets.text("BRONZE_CATALOG", "")

# COMMAND ----------

INGESTION_PATH = dbutils.widgets.get("INGESTION_PATH")
BRONZE_LAYER = dbutils.widgets.get("BRONZE_LAYER")
BRONZE_CATALOG = dbutils.widgets.get("BRONZE_CATALOG")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit


df_customers = (spark
    .read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(INGESTION_PATH)
)

df_customers = df_customers.withColumn("LOAD_TS", current_timestamp())
df_customers = df_customers.withColumn("LOAD_PATH", lit(INGESTION_PATH))


(df_customers
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(f"{BRONZE_CATALOG}.{BRONZE_LAYER}.source_online_products")
)
