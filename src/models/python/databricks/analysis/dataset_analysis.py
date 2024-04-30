# Databricks notebook source
import pandas as pd
from sklearn.preprocessing import OrdinalEncoder
from pyspark.sql.functions import array_join, col, concat_ws, lit

# COMMAND ----------

# MAGIC %run ../common/metrics_info

# COMMAND ----------

# MAGIC %run ../common/data_manipulation

# COMMAND ----------

# MAGIC %run ../../../../data_processing/python/databricks/common/DP_Tools/DP_Decrypter

# COMMAND ----------

df = spark.table("consumer_engagement_uc.dev_gold_db.online_consumer_full_gold")

cols_for_decryption = "consumer_city, consumer_age, consumer_phone_number"

df = get_decrypted_columns(df, cols_for_decryption)

df = cast_double_to_int(df)

df = df.drop("consumer_unique_id", "consumer_phone_number")

array_columns = [
    'payment_type_set',
    'product_category_name_english_set',
    'order_quality_label_set',
    'item_price_category_label_set',
    'payment_label_set',
    'product_volume_label_set'
]

for array_col in array_columns:
    df = df.withColumn(array_col, array_join(col(array_col), "', '"))
    df = df.withColumn(array_col, concat_ws("", lit("['"), col(array_col), lit("']")))

input_df = df.toPandas()

input_df['consumer_category'] = input_df['consumer_category'].replace({'Occasional': 0, 'Normal': 1, 'Best': 2})

columns_for_ordinal_encoder = ["consumer_country", "consumer_state", "consumer_city","consumer_age",
                               "consumer_age_label", "payment_type_set", "product_category_name_english_set", "order_quality_label_set","item_price_category_label_set", "payment_label_set", "product_volume_label_set", "most_frequent_payment_type"]

ordinal_encoder = OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1)

df_to_encode = input_df[columns_for_ordinal_encoder]

decoded_df = ordinal_encoder.fit_transform(df_to_encode)

input_df[columns_for_ordinal_encoder] = decoded_df

plt = plot_corr_matrix(input_df)

plt.show()

# COMMAND ----------

df = spark.table("consumer_engagement_uc.dev_gold_db.online_consumer_full_gold")

cols_for_decryption = "consumer_city, consumer_age, consumer_phone_number"

df = get_decrypted_columns(df, cols_for_decryption)

df = cast_double_to_int(df)

array_columns = [
    'payment_type_set',
    'product_category_name_english_set',
    'order_quality_label_set',
    'item_price_category_label_set',
    'payment_label_set',
    'product_volume_label_set'
]

for array_col in array_columns:
    df = df.withColumn(array_col, array_join(col(array_col), "', '"))
    df = df.withColumn(array_col, concat_ws("", lit("['"), col(array_col), lit("']")))

input_df = df.toPandas()

columns_for_model_training = ['consumer_zip_code', 'consumer_country', 'consumer_state', 'consumer_city',
                              'geolocation_latitude', 'geolocation_longitude', 'consumer_age', 'payment_type_set',
                              'consumer_payment_types_count', 'product_category_name_english_set',
                              'consumer_amount_of_orders', 'payment_label_set', "avg_product_price",
                              'avg_product_volume', 'avg_delivery_price',
                              'consumer_avg_payments_amount', "item_price_category_label_set",
                              'consumer_avg_review_score', 'consumer_amount_of_canceled_orders',
                              'consumer_amount_of_chip_items', 'consumer_amount_of_medium_items',
                              'consumer_amount_of_expensive_items', 'most_frequent_payment_type']

input_df = input_df[columns_for_model_training]

columns_for_ordinal_encoder = ["consumer_zip_code", "consumer_country", "consumer_state", "consumer_city", "item_price_category_label_set","payment_type_set", "consumer_age", "product_category_name_english_set", "payment_label_set", "most_frequent_payment_type"]

ordinal_encoder = OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1)

df_to_encode = input_df[columns_for_ordinal_encoder]

decoded_df = ordinal_encoder.fit_transform(df_to_encode)

input_df[columns_for_ordinal_encoder] = decoded_df

plt = plot_corr_matrix(input_df)

plt.show()

# COMMAND ----------

df = spark.table("consumer_engagement_uc.dev_gold_db.online_consumer_full_gold")

cols_for_decryption = "consumer_city, consumer_age, consumer_phone_number"

df = get_decrypted_columns(df, cols_for_decryption)

df = cast_double_to_int(df)

array_columns = [
    'payment_type_set',
    'product_category_name_english_set',
    'order_quality_label_set',
    'item_price_category_label_set',
    'payment_label_set',
    'product_volume_label_set'
]

for array_col in array_columns:
    df = df.withColumn(array_col, array_join(col(array_col), "', '"))
    df = df.withColumn(array_col, concat_ws("", lit("['"), col(array_col), lit("']")))

input_df = df.toPandas()

columns_for_model_training = ['consumer_zip_code', 'consumer_country', 'consumer_state', 'consumer_city',
                              'geolocation_latitude', 'geolocation_longitude', 'consumer_age', 'payment_type_set',
                              'consumer_payment_types_count', 'product_category_name_english_set', "consumer_category",
                              'consumer_amount_of_orders', 'payment_label_set', "avg_product_price",
                              'avg_product_volume', 'avg_delivery_price',
                              'consumer_avg_payments_amount', "item_price_category_label_set",
                              'consumer_avg_review_score', 'consumer_amount_of_canceled_orders',
                              'consumer_amount_of_chip_items', 'consumer_amount_of_medium_items',
                              'consumer_amount_of_expensive_items', 'most_frequent_payment_type']

input_df = input_df[columns_for_model_training]


input_df['consumer_category'] = input_df['consumer_category'].replace({'Occasional': 0, 'Normal': 1, 'Best': 2})

columns_for_ordinal_encoder = ["consumer_zip_code", "consumer_country", "consumer_state", "consumer_city", "item_price_category_label_set","payment_type_set", "consumer_age", "product_category_name_english_set", "payment_label_set", "most_frequent_payment_type"]

ordinal_encoder = OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1)

df_to_encode = input_df[columns_for_ordinal_encoder]

decoded_df = ordinal_encoder.fit_transform(df_to_encode)

input_df[columns_for_ordinal_encoder] = decoded_df

check_importances(input_df, 'consumer_category')
