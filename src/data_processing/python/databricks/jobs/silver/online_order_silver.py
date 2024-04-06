from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.functions import year, month, dayofmonth


spark = SparkSession\
    .builder\
    .appName("online_order_silver")\
    .master("local[*]")\
    .getOrCreate()

df_source_orders = spark\
    .read\
    .parquet('/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/bronze/source_online_order/')

# df_source_transactions = DP_Decrypter.get_decrypted_columns(df_source_transactions, "")

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

# online_orders = DP_Encrypter.get_encrypted_columns(online_orders, "")

online_orders\
    .write\
    .mode("overwrite")\
    .format("parquet")\
    .save("/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/silver/online_order/")