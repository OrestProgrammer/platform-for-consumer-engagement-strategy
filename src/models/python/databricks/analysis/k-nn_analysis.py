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
knn.fit(X_train_scaled, y_train)

y_pred_val = knn.predict(X_val_scaled)
y_pred_test = knn.predict(X_test_scaled)

accuracy_val = show_accuracy(y_val, y_pred_val)
accuracy_test = show_accuracy(y_test, y_pred_test)

precision_val = show_precision(y_val, y_pred_val)
precision_test = show_precision(y_test, y_pred_test)

recall_val = show_recall(y_val, y_pred_val)
recall_test = show_recall(y_test, y_pred_test)

f1_val = show_f1(y_val, y_pred_val)
f1_test = show_f1(y_test, y_pred_test)

table = PrettyTable()
table.field_names = ["Metric", "Validation result", "Test result"]

table.add_row(["Accuracy", accuracy_val, accuracy_test])
table.add_row(["Precision", precision_val, precision_test])
table.add_row(["Recall", recall_val, recall_test])
table.add_row(["F1 score", f1_val, f1_test])

print(table)

disp_cm_val = show_confusion_matrix(y_val, y_pred_val, knn)
disp_cm_test = show_confusion_matrix(y_test, y_pred_test, knn)

disp_cm_val.plot()
plt.title('Confusion Matrix for validation set')
plt.show()

disp_cm_test.plot()
plt.title('Confusion Matrix for test set')
plt.show()

lc_plt = plot_learning_curve(knn, X_train_scaled, y_train)
lc_plt.show()
