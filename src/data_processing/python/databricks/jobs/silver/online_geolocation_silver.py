from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession\
    .builder\
    .appName("online_geolocation_silver")\
    .master("local[*]")\
    .getOrCreate()

df_source_geolocation = spark\
    .read\
    .parquet('/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/bronze/source_online_geolocation/')

# df_source_geolocation = DP_Decrypter.get_decrypted_columns(df_source_geolocation, "")

online_geolocation = df_source_geolocation.select(
    col("geolocation_zip_code_prefix").cast("string").alias("geolocation_zip_code"),
    col("geolocation_lat").cast("double").alias("geolocation_latitude"),
    col("geolocation_lng").cast("double").alias("geolocation_longitude"),
    col("geolocation_city").cast("string").alias("geolocation_city"),
    col("geolocation_state").cast("string").alias("geolocation_state"),
).distinct()

# online_geolocation = DP_Encrypter.get_encrypted_columns(online_geolocation, "")

online_geolocation\
    .write\
    .mode("overwrite")\
    .format("parquet")\
    .save("/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/silver/online_geolocation/")