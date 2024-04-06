from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


spark = SparkSession\
    .builder\
    .appName("online_order_items_silver")\
    .master("local[*]")\
    .getOrCreate()

df_source_order_items = spark\
    .read\
    .parquet('/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/bronze/source_online_order_items/')

# df_source_transactions = DP_Decrypter.get_decrypted_columns(df_source_transactions, "")


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

# online_order_items = DP_Encrypter.get_encrypted_columns(online_order_items, "")

online_order_items\
    .write\
    .mode("overwrite")\
    .format("parquet")\
    .save("/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/silver/online_order_items/")