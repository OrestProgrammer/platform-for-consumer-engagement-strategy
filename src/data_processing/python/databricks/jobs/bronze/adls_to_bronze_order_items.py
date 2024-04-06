from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

spark = SparkSession\
    .builder\
    .appName("adls_to_bronze_order_items")\
    .master("local[*]")\
    .getOrCreate()

df_order_items = spark\
    .read\
    .csv('/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/source/source_order_items_dataset.csv', header=True, inferSchema=True)

df_order_items = df_order_items.withColumn("LOAD_TS", current_timestamp())
df_order_items = df_order_items.withColumn("LOAD_PATH", lit("data/source/source_order_items_dataset.csv"))

# df_transactions = DP_Encrypter.get_encrypted_columns(df_transactions, "")

df_order_items\
    .write\
    .format("parquet")\
    .mode("overwrite")\
    .save("/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/bronze/source_online_order_items/")