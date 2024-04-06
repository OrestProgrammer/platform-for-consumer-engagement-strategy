from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession\
    .builder\
    .appName("online_order_silver")\
    .master("local[*]")\
    .getOrCreate()

df_source_order_reviews = spark\
    .read\
    .parquet('/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/bronze/source_online_order_reviews/')

# df_source_order_reviews = DP_Decrypter.get_decrypted_columns(df_source_order_reviews, "")

df_source_order_reviews = df_source_order_reviews.filter(~col("review_id").rlike("[ :,\-]"))

online_order_reviews = df_source_order_reviews.select(
    col("review_id").cast("string").alias("review_id"),
    col("order_id").cast("string").alias("order_id"),
    col("review_score").cast("integer").alias("review_score"),
    col("review_comment_title").cast("string").alias("review_comment_title"),
    col("review_comment_message").cast("string").alias("review_comment_message"),
    col("review_creation_date").cast("timestamp").alias("review_creation_timestamp"),
    col("review_answer_timestamp").cast("timestamp").alias("review_answer_timestamp"),
).distinct()

# online_order_reviews = DP_Encrypter.get_encrypted_columns(online_order_reviews, "")

online_order_reviews\
    .write\
    .mode("overwrite")\
    .format("parquet")\
    .save("/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/silver/online_order_reviews/")