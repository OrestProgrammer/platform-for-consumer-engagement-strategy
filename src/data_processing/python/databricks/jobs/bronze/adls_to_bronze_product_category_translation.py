from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit


spark = SparkSession\
    .builder\
    .appName("adls_to_bronze_products")\
    .master("local[*]")\
    .getOrCreate()

df_product_translations = spark\
    .read\
    .csv('/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/source/source_product_category_name_translation.csv', header=True, inferSchema=True)

df_product_translations = df_product_translations.withColumn("LOAD_TS", current_timestamp())
df_product_translations = df_product_translations.withColumn("LOAD_PATH", lit("data/source/source_product_category_name_translation.csv"))

# df_product_translations = DP_Encrypter.get_encrypted_columns(df_product_translations, "")

df_product_translations\
    .write\
    .format("parquet")\
    .mode("overwrite")\
    .save("/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/bronze/source_online_product_category_name_translation/")