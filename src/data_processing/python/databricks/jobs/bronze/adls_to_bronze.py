# Databricks notebook source
dbutils.widgets.text("INGESTION_DIRECTORY", "")
dbutils.widgets.text("STORAGE_ACCOUNT", "")
dbutils.widgets.text("CONTAINER_NAME", "")
dbutils.widgets.text("BRONZE_CATALOG", "")
dbutils.widgets.text("BRONZE_LAYER", "")
dbutils.widgets.text("BRONZE_TABLE_NAME", "")
dbutils.widgets.text("PII_COLUMNS", "")
dbutils.widgets.text("FILE_FORMAT", "")
dbutils.widgets.text("FILE_SUB_DIRECTORY", "")
dbutils.widgets.text("IS_HISTORICAL_LOAD", "")

# COMMAND ----------

INGESTION_DIRECTORY = dbutils.widgets.get("INGESTION_DIRECTORY")
STORAGE_ACCOUNT = dbutils.widgets.get("STORAGE_ACCOUNT")
CONTAINER_NAME = dbutils.widgets.get("CONTAINER_NAME")
BRONZE_CATALOG = dbutils.widgets.get("BRONZE_CATALOG")
BRONZE_LAYER = dbutils.widgets.get("BRONZE_LAYER")
BRONZE_TABLE_NAME = dbutils.widgets.get("BRONZE_TABLE_NAME")
PII_COLUMNS = dbutils.widgets.get("PII_COLUMNS")
FILE_FORMAT = dbutils.widgets.get("FILE_FORMAT")
FILE_SUB_DIRECTORY = dbutils.widgets.get("FILE_SUB_DIRECTORY")
IS_HISTORICAL_LOAD = dbutils.widgets.get("IS_HISTORICAL_LOAD")

# COMMAND ----------

# MAGIC %run ../../common/DP_Tools/DP_Encrypter

# COMMAND ----------

if IS_HISTORICAL_LOAD == "Y":
    INGESTION_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{INGESTION_DIRECTORY}/historical_data/"
else:
    INGESTION_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{INGESTION_DIRECTORY}/{FILE_SUB_DIRECTORY}/"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

if FILE_FORMAT == "CSV":
    df_customers = (spark
        .read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(INGESTION_PATH)
    )

df_customers = df_customers.withColumn("LOAD_TS", current_timestamp())
df_customers = df_customers.withColumn("LOAD_PATH", lit(INGESTION_PATH))

if PII_COLUMNS.strip():
    df_customers = get_encrypted_columns(df_customers, PII_COLUMNS)

(df_customers
 .write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(f"{BRONZE_CATALOG}.{BRONZE_LAYER}.{BRONZE_TABLE_NAME}")
)
