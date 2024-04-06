from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


spark = SparkSession\
    .builder\
    .appName("online_order_silver")\
    .master("local[*]")\
    .getOrCreate()

df_source_products = spark\
    .read\
    .parquet('/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/bronze/source_online_products/')

# df_source_products = DP_Decrypter.get_decrypted_columns(df_source_products, "")

online_products = df_source_products.select(
    col("product_id").cast("string").alias("product_id"),
    col("product_category_name").cast("string").alias("product_category_name"),
    col("product_name_lenght").cast("integer").alias("product_name_length"),
    col("product_description_lenght").cast("integer").alias("product_description_length"),
    col("product_photos_qty").cast("integer").alias("product_photos_amount"),
    col("product_weight_g").cast("integer").alias("product_weight_g"),
    col("product_length_cm").cast("integer").alias("product_length_cm"),
    col("product_height_cm").cast("integer").alias("product_height_cm"),
    col("product_width_cm").cast("integer").alias("product_width_cm"),
).distinct()

online_products = online_products.withColumn("product_volume_cm3",
                   col("product_length_cm") * col("product_height_cm") * col("product_width_cm"))

online_products = online_products.withColumn('product_volume_label',
                   when(col('product_volume_cm3') <= 10000, 'SV')
                   .when((col('product_volume_cm3') > 10000) & (col('product_volume_cm3') <= 150000), 'MV')
                   .otherwise('LV'))

# online_products = DP_Encrypter.get_encrypted_columns(online_products, "")

online_products\
    .write\
    .mode("overwrite")\
    .format("parquet")\
    .save("/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/silver/online_products/")