from pyspark.sql.functions import expr

encryption_key = '4LHMmmssuCUR9bvYdbo9qcZpWG4aExPU'


def get_decrypted_columns(df, columns_to_decrypt):
    lst = columns_to_decrypt.split(',')
    for i in lst:
        df = df.withColumn(i, expr(f"aes_decrypt(unbase64({i}), '{encryption_key}', 'CBC', 'PKCS')").cast("string"))

    return df