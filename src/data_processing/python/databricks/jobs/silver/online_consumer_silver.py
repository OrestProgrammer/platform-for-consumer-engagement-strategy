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

# MAGIC %run ../../common/DP_Tools/DP_Encrypter

# COMMAND ----------

# MAGIC %run ../../common/DP_Tools/DP_Decrypter

# COMMAND ----------

from pyspark.sql.functions import when, col


df_source_customers = spark.table(f"{BRONZE_CATALOG}.{BRONZE_LAYER}.source_online_consumer")

cols_for_decryption = "customer_city, customer_age, customer_phone_number"

df_source_customers = get_decrypted_columns(df_source_customers, cols_for_decryption)

online_consumer = df_source_customers.select(
    col("customer_unique_id").cast("string").alias("consumer_unique_id"),
    col("customer_zip_code_prefix").cast("string").alias("consumer_zip_code"),
    col("customer_country").cast("string").alias("consumer_country"),
    col("customer_state").cast("string").alias("consumer_state"),
    col("customer_city").cast("string").alias("consumer_city"),
    col("customer_age").cast("string").alias("consumer_age"),
    col("customer_phone_number").cast("string").alias("consumer_phone_number"),
    col("customer_category").cast("string").alias("consumer_category")
).distinct()


online_consumer = online_consumer\
    .withColumn("consumer_age_label",
                when(col("consumer_age") < 24, "YC")
                .when((col("consumer_age") >= 24) & (col("consumer_age") < 30), "MC")
                .when(col("consumer_age") >= 30, "AC")
                .otherwise("D"))
    
cols_for_encryption = "consumer_city, consumer_age, consumer_phone_number"

online_consumer = get_encrypted_columns(online_consumer, cols_for_encryption)

(online_consumer
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(f"{SILVER_CATALOG}.{SILVER_LAYER}.online_consumer")
)
