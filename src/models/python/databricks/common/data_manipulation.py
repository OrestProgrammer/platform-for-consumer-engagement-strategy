# Databricks notebook source
from sklearn.preprocessing import OrdinalEncoder
from pyspark.sql.types import DecimalType


def data_preprocessing(input_df):

    columns_for_model_training = ["consumer_amount_of_orders", "consumer_avg_payments_amount", "item_price_category_label_set", "product_category_name_english_set", "consumer_amount_of_medium_items", "avg_product_price", "payment_label_set", "consumer_state",
    "consumer_avg_review_score", "consumer_amount_of_canceled_orders", "consumer_amount_of_chip_items", "consumer_amount_of_expensive_items","consumer_age", "consumer_category"]

    input_df = input_df[columns_for_model_training]

    input_df = input_df.dropna()

    input_df['consumer_category'] = input_df['consumer_category'].replace({'Occasional': 0, 'Normal': 1, 'Best': 2})

    columns_for_ordinal_encoder = ["item_price_category_label_set", "product_category_name_english_set", "payment_label_set", "consumer_state","consumer_age"]

    ordinal_encoder = OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1)

    df_to_encode = input_df[columns_for_ordinal_encoder]

    decoded_df = ordinal_encoder.fit_transform(df_to_encode)

    input_df[columns_for_ordinal_encoder] = decoded_df

    return input_df


def cast_double_to_int(df):
    for column in df.schema:
        if isinstance(column.dataType, DecimalType):
            df = df.withColumn(column.name, df[column.name].cast('int'))
    return df
