# Databricks notebook source
pip install prettytable

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import train_test_split
from prettytable import PrettyTable
import matplotlib.pyplot as plt
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

array_columns = [
    'product_category_name_english_set',
    'item_price_category_label_set',
    'payment_label_set'
]

for array_col in array_columns:
    df = df.withColumn(array_col, array_join(col(array_col), "', '"))
    df = df.withColumn(array_col, concat_ws("", lit("['"), col(array_col), lit("']")))

input_df = df.toPandas()

processed_df = data_preprocessing(input_df)

X = processed_df.drop("consumer_category", axis=1)
y = processed_df["consumer_category"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=30)

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

knn = KNeighborsClassifier(n_neighbors=5, weights='uniform', algorithm='ball_tree', leaf_size=20, metric='minkowski', p=2)
knn.fit(X_train_scaled, y_train)

y_pred_train = knn.predict(X_train_scaled)
y_pred_test = knn.predict(X_test_scaled)

accuracy_train = show_accuracy(y_train, y_pred_train)
accuracy_test = show_accuracy(y_test, y_pred_test)

precision_train = show_precision(y_train, y_pred_train)
precision_test = show_precision(y_test, y_pred_test)

recall_train = show_recall(y_train, y_pred_train)
recall_test = show_recall(y_test, y_pred_test)

f1_train = show_f1(y_train, y_pred_train)
f1_test = show_f1(y_test, y_pred_test)

table = PrettyTable()
table.field_names = ["Metric", "Train result", "Test result"]

table.add_row(["Accuracy", accuracy_train, accuracy_test])
table.add_row(["Precision", precision_train, precision_test])
table.add_row(["Recall", recall_train, recall_test])
table.add_row(["F1 score", f1_train, f1_test])

print(table)

disp_cm_train = show_confusion_matrix(y_train, y_pred_train, knn)
disp_cm_test = show_confusion_matrix(y_test, y_pred_test, knn)

disp_cm_train.plot()
plt.title('Confusion Matrix for train set')
plt.show()

disp_cm_test.plot()
plt.title('Confusion Matrix for test set')
plt.show()

lc_plt = plot_learning_curve(knn, X_scaled, y)
lc_plt.show()

# COMMAND ----------


