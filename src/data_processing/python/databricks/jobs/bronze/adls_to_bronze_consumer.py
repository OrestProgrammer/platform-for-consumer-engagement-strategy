from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit


spark = SparkSession\
    .builder\
    .appName("adls_to_bronze_consumers")\
    .master("local[*]")\
    .getOrCreate()

df_customers = spark\
    .read\
    .csv('/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/source/source_customers_dataset.csv', header=True, inferSchema=True)

df_customers = df_customers.withColumn("LOAD_TS", current_timestamp())
df_customers = df_customers.withColumn("LOAD_PATH", lit("data/source/source_customers_dataset.csv"))

# df_customers = DP_Encrypter.get_encrypted_columns(df_customers, "")

df_customers\
    .write\
    .format("parquet") \
    .mode("overwrite") \
    .save("/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/bronze/source_online_consumer/")