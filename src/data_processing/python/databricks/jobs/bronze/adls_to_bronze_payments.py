from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit


spark = SparkSession\
    .builder\
    .appName("adls_to_bronze_payments")\
    .master("local[*]")\
    .getOrCreate()

df_payments = spark\
    .read\
    .csv('/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/source/source_order_payments_dataset.csv', header=True, inferSchema=True)

df_payments = df_payments.withColumn("LOAD_TS", current_timestamp())
df_payments = df_payments.withColumn("LOAD_PATH", lit("data/source/source_order_payments_dataset.csv"))

# df_transactions = DP_Encrypter.get_encrypted_columns(df_transactions, "")

df_payments\
    .write\
    .format("parquet")\
    .mode("overwrite")\
    .save("/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/bronze/source_online_payments/")