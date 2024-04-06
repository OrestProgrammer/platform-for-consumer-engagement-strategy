from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


spark = SparkSession\
    .builder\
    .appName("online_order_silver")\
    .master("local[*]")\
    .getOrCreate()

df_source_payments = spark\
    .read\
    .parquet('/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/bronze/source_online_payments/')

# df_source_transactions = DP_Decrypter.get_decrypted_columns(df_source_transactions, "")


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


# online_orders = DP_Encrypter.get_encrypted_columns(online_orders, "")

online_payments\
    .write\
    .mode("overwrite")\
    .format("parquet")\
    .save("/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/silver/online_payments/")