from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit


spark = SparkSession\
    .builder\
    .appName("adls_to_bronze_products")\
    .master("local[*]")\
    .getOrCreate()

df_order_reviews = spark\
    .read\
    .csv('/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/source/source_order_reviews_dataset.csv', header=True, inferSchema=True)

df_order_reviews = df_order_reviews.withColumn("LOAD_TS", current_timestamp())
df_order_reviews = df_order_reviews.withColumn("LOAD_PATH", lit("data/source/source_order_reviews_dataset.csv"))

# df_order_reviews = DP_Encrypter.get_encrypted_columns(df_order_reviews, "")

df_order_reviews\
    .write\
    .format("parquet")\
    .mode("overwrite")\
    .save("/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/bronze/source_online_order_reviews/")