# Databricks notebook source
pip install scikit-learn==1.3.2

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("GOLD_CATALOG", "")
dbutils.widgets.text("GOLD_LAYER", "")
dbutils.widgets.text("PATH_TO_SAVE_MODEL", "")

# COMMAND ----------

GOLD_CATALOG = dbutils.widgets.get("GOLD_CATALOG")
GOLD_LAYER = dbutils.widgets.get("GOLD_LAYER")
PATH_TO_SAVE_MODEL = dbutils.widgets.get("PATH_TO_SAVE_MODEL")

# COMMAND ----------

import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier, VotingClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.svm import SVC
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
import pickle
import os
from pyspark.sql.functions import array_join, col, concat_ws, lit
import base64

# COMMAND ----------

# MAGIC %run ../common/data_manipulation

# COMMAND ----------

# MAGIC %run ../../../../data_processing/python/databricks/common/DP_Tools/DP_Decrypter

# COMMAND ----------

df = spark.table(f"{GOLD_CATALOG}.{GOLD_LAYER}.online_consumer_full_gold")

cols_for_decryption = "consumer_city, consumer_age, consumer_phone_number"

df = get_decrypted_columns(df, cols_for_decryption)

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

processed_df = data_preprocessing(input_df)

X = processed_df.drop("consumer_category", axis=1)
y = processed_df["consumer_category"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=30)
X_train, X_val, y_train, y_val = train_test_split(X_train, y_train, test_size=0.2, random_state=30)

scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)
X_val_scaled = scaler.transform(X_val)

knn = KNeighborsClassifier(n_neighbors=3, weights='distance', algorithm='auto', leaf_size=20, metric='euclidean', p=1)
svm = SVC(C=5.0, kernel='poly', gamma='auto', degree=4, coef0=0.0, decision_function_shape='ovr', shrinking=True,
          probability=True, max_iter=-1)
rf = RandomForestClassifier(n_estimators=50, criterion='entropy', max_depth=5, min_samples_split=3, min_samples_leaf=2, max_features='sqrt', max_leaf_nodes=3, class_weight='balanced', random_state=1)
dt = DecisionTreeClassifier(criterion='gini', max_depth=10, min_samples_split=3, min_samples_leaf=2, max_features='sqrt', splitter='random', random_state=1)

ensemble = VotingClassifier(estimators=[('knn', knn), ('svm', svm), ('rf', rf), ('dt', dt)], voting='soft')
ensemble.fit(X_train_scaled, y_train)

adls_model_path = f"{PATH_TO_SAVE_MODEL}/model.pkl"

modelpkl = pickle.dumps(ensemble)

model_base64_encoded = base64.b64encode(modelpkl).decode('utf-8')

dbutils.fs.put(adls_model_path, model_base64_encoded, overwrite=True)
