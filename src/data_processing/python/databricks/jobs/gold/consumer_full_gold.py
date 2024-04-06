from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_set, size, when, collect_list, array_max, array_min, expr, transform, sort_array

spark = SparkSession \
    .builder \
    .appName("consumer_full_gold") \
    .master("local[*]") \
    .getOrCreate()

df_online_consumer = spark.read.parquet(
    "/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/silver/online_consumer/")
df_online_geolocation = spark.read.parquet(
    "/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/silver/online_geolocation/")
df_online_order = spark.read.parquet(
    "/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/silver/online_order/")
df_online_order_items = spark.read.parquet(
    "/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/silver/online_order_items/")
df_online_order_reviews = spark.read.parquet(
    "/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/silver/online_order_reviews/")
df_online_payments = spark.read.parquet(
    "/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/silver/online_payments/")
df_online_product_category_translation_silver = spark.read.parquet(
    "/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/silver/online_product_category_translation_silver/")
df_online_products = spark.read.parquet(
    "/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/silver/online_products/")

# df_online_consumer = DP_Decrypter.get_decrypted_columns(df_online_consumer, "")
# df_online_geolocation = DP_Decrypter.get_decrypted_columns(df_online_geolocation, "")
# df_online_order = DP_Decrypter.get_decrypted_columns(df_online_order, "")
# df_online_order_items = DP_Decrypter.get_decrypted_columns(df_online_order_items, "")
# df_online_order_reviews = DP_Decrypter.get_decrypted_columns(df_online_order_reviews, "")
# df_online_payments = DP_Decrypter.get_decrypted_columns(df_online_payments, "")
# df_online_product_category_translation_silver = DP_Decrypter.get_decrypted_columns(df_online_product_category_translation_silver, "")
# df_online_products = DP_Decrypter.get_decrypted_columns(df_online_products, "")

df_online_products_joined_df_online_product_category_translation_silver = df_online_products \
    .join(df_online_product_category_translation_silver,
          df_online_products["product_category_name"] == df_online_product_category_translation_silver[
              "product_category_name_original"], how='left')

df_products = df_online_products_joined_df_online_product_category_translation_silver.drop(
    df_online_products["product_category_name"],
    df_online_product_category_translation_silver["product_category_name_original"])

df_online_order_items_joined_df_products = df_online_order_items.join(df_products,
                                                                      df_online_order_items["product_id"] ==
                                                                      df_products["product_id"], how='left')

df_order_items = df_online_order_items_joined_df_products.drop(df_products["product_id"])

df_online_order_joined_df_online_order_items = df_online_order.join(df_order_items,
                                                                    df_order_items["order_id"] == df_online_order[
                                                                        "order_id"], how='left')

df_orders = df_online_order_joined_df_online_order_items.drop(df_online_order["order_id"])

df_orders_joined_df_online_payments = df_orders.join(df_online_payments,
                                                     df_orders["order_id"] == df_online_payments["order_id"],
                                                     how='left')

df_orders = df_orders_joined_df_online_payments.drop(df_online_payments["order_id"])

df_orders_joined_df_online_order_reviews = df_orders.join(df_online_order_reviews,
                                                          df_orders["order_id"] == df_online_order_reviews["order_id"],
                                                          how='left')

df_orders = df_orders_joined_df_online_order_reviews.drop(df_online_order_reviews["order_id"])

df_online_consumer_joined_df_online_geolocation = df_online_consumer.join(
    df_online_geolocation,
    (df_online_consumer["consumer_zip_code"] == df_online_geolocation["geolocation_zip_code"]) &
    (df_online_consumer["consumer_state"] == df_online_geolocation["geolocation_state"]) &
    (df_online_consumer["consumer_city"] == df_online_geolocation["geolocation_city"]),
    how='left'
)

df_consumers = df_online_consumer_joined_df_online_geolocation.drop(df_online_geolocation["geolocation_zip_code"],
                                                                    df_online_geolocation["geolocation_state"],
                                                                    df_online_geolocation["geolocation_city"])

df_consumer_joined_orders = df_orders.join(df_consumers,
                                           df_orders["consumer_unique_id"] == df_consumers["consumer_unique_id"],
                                           how='left')

df_consumer_orders = df_consumer_joined_orders.drop(df_consumers["consumer_unique_id"])

df_consumer_orders_pre_final = df_consumer_orders.select(
    col("consumer_unique_id"),
    col("consumer_zip_code"),
    col("consumer_country"),
    col("consumer_state"),
    col("consumer_city"),
    col("consumer_age"),
    col("geolocation_latitude"),
    col("geolocation_longitude"),
    col("order_id"),
    col("product_id"),
    col("item_price"),
    col("item_freight_value"),
    col("product_weight_g"),
    col("product_volume_cm3"),
    col("product_category_name_english"),
    col("payment_type"),
    col("payment_value"),
    col("review_id"),
    col("review_score"),
    col("review_comment_message"),
    col("consumer_age_label"),
    col("item_price_category_label"),
    col("order_quality_label"),
    col("product_volume_label"),
    col("payment_label"),
    col("consumer_category"),
).distinct()

exclude_columns = ['consumer_unique_id', 'consumer_zip_code', 'consumer_country', 'consumer_state', 'consumer_city',
                   'consumer_age', 'geolocation_latitude', 'geolocation_longitude',
                   'consumer_age_label', 'consumer_category']

cols_for_list = ["order_id",
                 "product_id",
                 "item_price",
                 "item_freight_value",
                 "product_weight_g",
                 "product_volume_cm3",
                 "payment_type",
                 "payment_value",
                 "review_score",
                 "review_id",
                 "review_comment_message",
                 "item_price_category_label",
                 "order_quality_label",
                 ]

cols_for_set = ["payment_type",
                "product_category_name_english",
                "order_quality_label",
                "item_price_category_label",
                "payment_label",
                "product_volume_label"
                ]

agg_exprs_list = [sort_array(collect_list(col)).alias(f'{col}_list') for col in cols_for_list]
agg_exprs_set = [sort_array(collect_set(col)).alias(f'{col}_set') for col in cols_for_set]

df_consumer_orders_final = df_consumer_orders_pre_final.groupBy(exclude_columns).agg(*agg_exprs_list, *agg_exprs_set)

df_consumer_orders_final = df_consumer_orders_final \
    .withColumn("consumer_amount_of_orders", size(expr("array_distinct(order_id_list)"))) \
    .withColumn("consumer_amount_of_products", size(expr("array_distinct(product_id_list)"))) \
    .withColumn("max_product_price", array_max(df_consumer_orders_final["item_price_list"])) \
    .withColumn("min_product_price", array_min(df_consumer_orders_final["item_price_list"])) \
    .withColumn("item_price_list_double",
                transform(df_consumer_orders_final["item_price_list"], lambda x: x.cast("double"))) \
    .withColumn("avg_product_price", expr("AGGREGATE(item_price_list_double, CAST (0 AS DOUBLE), (a, b) -> a + b, "
                                          "a -> a / size(item_price_list_double))").cast("decimal(14,2)")) \
    .withColumn("max_delivery_price", array_max(df_consumer_orders_final["item_freight_value_list"])) \
    .withColumn("min_delivery_price", array_min(df_consumer_orders_final["item_freight_value_list"])) \
    .withColumn("item_freight_value_list_double",
                transform(df_consumer_orders_final["item_freight_value_list"], lambda x: x.cast("double"))) \
    .withColumn("avg_delivery_price", expr("AGGREGATE(item_freight_value_list_double, CAST (0 AS DOUBLE), (a, b) -> a "
                                           "+ b, a -> a / size(item_freight_value_list_double))").cast("decimal(14,2)")) \
    .withColumn("total_delivery_price",
                expr("AGGREGATE(item_freight_value_list_double, CAST (0 AS DOUBLE), (a, b) -> a + b)").cast(
                    "decimal(14,2)")) \
    .withColumn("consumer_max_payments_amount", array_max(df_consumer_orders_final["payment_value_list"])) \
    .withColumn("consumer_min_payments_amount", array_min(df_consumer_orders_final["payment_value_list"])) \
    .withColumn("payment_value_list_double",
                transform(df_consumer_orders_final["payment_value_list"], lambda x: x.cast("double"))) \
    .withColumn("consumer_avg_payments_amount",
                expr("AGGREGATE(payment_value_list_double, CAST (0 AS DOUBLE), (a, b) -> a "
                     "+ b, a -> a / size(payment_value_list_double))").cast("decimal(14,2)")) \
    .withColumn("consumer_total_payments_amount",
                expr("AGGREGATE(payment_value_list_double, CAST (0 AS DOUBLE), (a, b) -> a + b)").cast("decimal(14,2)")) \
    .withColumn("consumer_payment_types_count", expr("size(payment_type_set)")) \
    .withColumn("credit_card_payments_amount",
                expr(
                    "SIZE(FILTER(payment_type_list, x -> x = 'credit_card'))")) \
    .withColumn("boleto_payments_amount",
                expr("SIZE(FILTER(payment_type_list, x -> x = 'boleto'))")) \
    .withColumn("voucher_payments_amount",
                expr("SIZE(FILTER(payment_type_list, x -> x = 'voucher'))")) \
    .withColumn("debit_card_payments_amount",
                expr("SIZE(FILTER(payment_type_list, x -> x = 'debit_card'))")) \
    .withColumn("consumer_amount_of_reviews", size(df_consumer_orders_final["review_id_list"])) \
    .withColumn("max_product_weight", array_max(df_consumer_orders_final["product_weight_g_list"])) \
    .withColumn("min_product_weight", array_min(df_consumer_orders_final["product_weight_g_list"])) \
    .withColumn("avg_product_weight_g", expr("AGGREGATE(product_weight_g_list, CAST(0 AS DOUBLE), (acc, x) -> acc + "
                                             "x, acc -> acc / size(product_weight_g_list))").cast("decimal(14,2)"))\
    .withColumn("max_product_volume", array_max(df_consumer_orders_final["product_volume_cm3_list"])) \
    .withColumn("min_product_volume", array_min(df_consumer_orders_final["product_volume_cm3_list"])) \
    .withColumn("avg_product_volume", expr("AGGREGATE(product_volume_cm3_list, CAST(0 AS DOUBLE), (acc, x) -> acc + "
                                             "x, acc -> acc / size(product_volume_cm3_list))").cast("decimal(14,2)"))\
    .withColumn("consumer_max_review_score", array_max(df_consumer_orders_final["review_score_list"])) \
    .withColumn("consumer_min_review_score", array_min(df_consumer_orders_final["review_score_list"])) \
    .withColumn("consumer_avg_review_score", expr("AGGREGATE(review_score_list, CAST(0 AS DOUBLE), (acc, x) -> acc + "
                                             "x, acc -> acc / size(review_score_list))").cast("decimal(14,2)"))\
    .withColumn("consumer_amount_of_feedbacks", size(expr("array_distinct(review_comment_message_list)")))\
    .withColumn("consumer_amount_of_good_orders",
                expr(
                    "SIZE(FILTER(order_quality_label_list, x -> x = 'GO'))")) \
    .withColumn("consumer_amount_of_canceled_orders",
                expr("SIZE(FILTER(order_quality_label_list, x -> x = 'CO'))")) \
    .withColumn("consumer_amount_of_chip_items",
                expr(
                    "SIZE(FILTER(item_price_category_label_list, x -> x = 'CI'))")) \
    .withColumn("consumer_amount_of_medium_items",
                expr("SIZE(FILTER(item_price_category_label_list, x -> x = 'MI'))")) \
    .withColumn("consumer_amount_of_expensive_items",
                expr("SIZE(FILTER(item_price_category_label_list, x -> x = 'EI'))")) \


df_consumer_orders_final = df_consumer_orders_final.withColumn('most_frequent_payment_type',
                                                               when(
                                                                   (df_consumer_orders_final[
                                                                        'credit_card_payments_amount'] >=
                                                                    df_consumer_orders_final[
                                                                        'boleto_payments_amount']) &
                                                                   (df_consumer_orders_final[
                                                                        'credit_card_payments_amount'] >=
                                                                    df_consumer_orders_final[
                                                                        'voucher_payments_amount']) &
                                                                   (df_consumer_orders_final[
                                                                        'credit_card_payments_amount'] >=
                                                                    df_consumer_orders_final[
                                                                        'debit_card_payments_amount']), 'credit_card')
                                                               .when(
                                                                   (df_consumer_orders_final[
                                                                        'boleto_payments_amount'] >=
                                                                    df_consumer_orders_final[
                                                                        'credit_card_payments_amount']) &
                                                                   (df_consumer_orders_final[
                                                                        'boleto_payments_amount'] >=
                                                                    df_consumer_orders_final[
                                                                        'voucher_payments_amount']) &
                                                                   (df_consumer_orders_final[
                                                                        'boleto_payments_amount'] >=
                                                                    df_consumer_orders_final[
                                                                        'debit_card_payments_amount']), 'boleto')
                                                               .when(
                                                                   (df_consumer_orders_final[
                                                                        'voucher_payments_amount'] >=
                                                                    df_consumer_orders_final[
                                                                        'credit_card_payments_amount']) &
                                                                   (df_consumer_orders_final[
                                                                        'voucher_payments_amount'] >=
                                                                    df_consumer_orders_final[
                                                                        'boleto_payments_amount']) &
                                                                   (df_consumer_orders_final[
                                                                        'voucher_payments_amount'] >=
                                                                    df_consumer_orders_final[
                                                                        'debit_card_payments_amount']), 'voucher')
                                                               .otherwise('debit_card'))


df_consumer_orders_final = df_consumer_orders_final.drop("consumer_unique_id",
                                                         "order_id_list",
                                                         "product_id_list",
                                                         "item_price_list",
                                                         "item_price_list_double",
                                                         "item_freight_value_list_double",
                                                         "item_freight_value_list",
                                                         "payment_value_list_double",
                                                         "payment_value_list",
                                                         "payment_type_list",
                                                         "review_id_list",
                                                         "product_weight_g_list",
                                                         "product_volume_cm3_list",
                                                         "review_score_list",
                                                         "review_comment_message_list",
                                                         "order_quality_label_list",
                                                         "item_price_category_label_list")


df_consumer_orders_final = df_consumer_orders_final.distinct()

df_consumer_orders_final = df_consumer_orders_final.dropna()

# df_consumer_orders_final = DP_Encrypter.get_encrypted_columns(df_consumer_orders_final, "")

df_consumer_orders_final\
    .write\
    .mode("overwrite")\
    .format("parquet")\
    .save("/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/gold/online_consumer_full_gold/")

df_consumer_orders_final.toPandas().to_csv('/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/gold/online_consumer_full_gold.csv', index = False)