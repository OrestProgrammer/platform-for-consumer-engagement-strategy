# Databricks notebook source
from pyspark.sql.functions import expr

encryption_key = dbutils.secrets.get(scope="consumer-engagement-scope", key="encryption-key")


def get_decrypted_columns(df, columns_to_decrypt):
    lst = columns_to_decrypt.split(',')
    for i in lst:
        df = df.withColumn(i.strip(), expr(f"aes_decrypt(unbase64({i.strip()}), '{encryption_key}', 'CBC', 'PKCS')").cast("string"))

    return df
