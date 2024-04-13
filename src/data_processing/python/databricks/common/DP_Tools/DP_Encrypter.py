# Databricks notebook source
from pyspark.sql.functions import expr, base64, col
from pyspark.sql.types import StringType

encryption_key = dbutils.secrets.get(scope="consumer-engagement-scope", key="encryption-key")


def get_encrypted_columns(df, columns_to_encrypt):
    lst = columns_to_encrypt.split(',')
    for i in lst:
        df = df.withColumn(i.strip(), base64(expr(f"aes_encrypt({i.strip()}, '{encryption_key}', 'CBC', 'PKCS')")))

    return df
