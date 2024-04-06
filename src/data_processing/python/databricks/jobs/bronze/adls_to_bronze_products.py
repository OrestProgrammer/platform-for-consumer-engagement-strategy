from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit


spark = SparkSession\
    .builder\
    .appName("adls_to_bronze_products")\
    .master("local[*]")\
    .getOrCreate()

df_products = spark\
    .read\
    .csv('/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/source/source_products_dataset.csv', header=True, inferSchema=True)

df_products = df_products.withColumn("LOAD_TS", current_timestamp())
df_products = df_products.withColumn("LOAD_PATH", lit("data/source/source_products_dataset.csv"))

# df_products = DP_Encrypter.get_encrypted_columns(df_products, "")

df_products\
    .write\
    .format("parquet")\
    .mode("overwrite")\
    .save("/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/bronze/source_online_products/")