from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession\
    .builder\
    .appName("online_order_silver")\
    .master("local[*]")\
    .getOrCreate()

df_source_product_translations = spark\
    .read\
    .parquet('/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/bronze/source_online_product_category_name_translation/')

# df_source_product_translations = DP_Decrypter.get_decrypted_columns(df_source_product_translations, "")

online_product_category_translation_silver = df_source_product_translations.select(
    col("product_category_name").cast("string").alias("product_category_name_original"),
    col("product_category_name_english").cast("string").alias("product_category_name_english"),
).distinct()

# online_product_category_translation_silver = DP_Encrypter.get_encrypted_columns(online_product_category_translation_silver, "")

online_product_category_translation_silver\
    .write\
    .mode("overwrite")\
    .format("parquet")\
    .save("/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/silver/online_product_category_translation_silver/")