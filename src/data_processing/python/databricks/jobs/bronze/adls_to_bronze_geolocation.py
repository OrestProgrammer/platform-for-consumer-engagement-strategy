from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col


spark = SparkSession\
    .builder\
    .appName("adls_to_bronze_geolocation")\
    .master("local[*]")\
    .getOrCreate()

df_geolocations = spark\
    .read\
    .csv('/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/source/source_geolocation_dataset.csv', header=True, inferSchema=True)

df_geolocations = df_geolocations.withColumn("LOAD_TS", current_timestamp())
df_geolocations = df_geolocations.withColumn("LOAD_PATH", lit("data/source/source_geolocation_dataset.csv"))

# df_geolocations = DP_Encrypter.get_encrypted_columns(df_geolocations, "")

df_geolocations\
    .write\
    .format("parquet")\
    .mode("overwrite")\
    .save("/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/bronze/source_online_geolocation/")