from pyspark.sql.functions import expr, base64

encryption_key = '4LHMmmssuCUR9bvYdbo9qcZpWG4aExPU'


def get_encrypted_columns(df, columns_to_encrypt):
    lst = columns_to_encrypt.split(',')
    for i in lst:
        df = df.withColumn(i, base64(expr(f"aes_encrypt({i}, '{encryption_key}', 'CBC', 'PKCS')")))

    return df