from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


spark = SparkSession\
    .builder\
    .appName("online_consumer_silver")\
    .master("local[*]")\
    .getOrCreate()

df_source_customers = spark\
    .read\
    .parquet('/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/bronze/source_online_consumer/')

# df_source_customers = DP_Decrypter.get_decrypted_columns(df_source_customers, "")

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

# online_consumer = DP_Encrypter.get_encrypted_columns(online_consumer, "")

online_consumer\
    .write\
    .mode("overwrite")\
    .format("parquet")\
    .save("/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/silver/online_consumer/")