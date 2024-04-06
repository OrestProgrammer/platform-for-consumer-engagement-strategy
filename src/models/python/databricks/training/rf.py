import pandas as pd
import numpy as np
from sklearn.preprocessing import OrdinalEncoder, StandardScaler, label_binarize
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from prettytable import PrettyTable
import src.models.python.databricks.common.metrics_info as mi
import matplotlib.pyplot as plt

input_df = pd.read_csv('/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/gold/online_consumer_full_gold.csv')

input_df = input_df.dropna()

input_df['consumer_category'] = input_df['consumer_category'].replace({'Occasional': 0, 'Normal': 1, 'Best': 2})

columns_for_ordinal_encoder = ["consumer_country", "consumer_state", "consumer_city", "consumer_age_label",
                               "payment_type_set", "product_category_name_english_set", "order_quality_label_set",
                               "item_price_category_label_set", "payment_label_set", "product_volume_label_set",
                               "most_frequent_payment_type"]

ordinal_encoder = OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1)

df_to_encode = input_df[columns_for_ordinal_encoder]

decoded_df = ordinal_encoder.fit_transform(df_to_encode)

input_df[columns_for_ordinal_encoder] = decoded_df

X = input_df.drop("consumer_category", axis=1)
y = input_df["consumer_category"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=30)
X_train, X_val, y_train, y_val = train_test_split(X_train, y_train, test_size=0.2, random_state=30)

scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)
X_val_scaled = scaler.transform(X_val)

rf = RandomForestClassifier(n_estimators=50, criterion='entropy', max_depth=5, min_samples_split=3, min_samples_leaf=2, max_features='sqrt', max_leaf_nodes=2, class_weight='balanced', random_state=1)
rf.fit(X_train_scaled, y_train)

y_pred_val = rf.predict(X_val_scaled)
y_pred_test = rf.predict(X_test_scaled)

accuracy_val = mi.show_accuracy(y_val, y_pred_val)
accuracy_test = mi.show_accuracy(y_test, y_pred_test)

precision_val = mi.show_precision(y_val, y_pred_val)
precision_test = mi.show_precision(y_test, y_pred_test)

recall_val = mi.show_recall(y_val, y_pred_val)
recall_test = mi.show_recall(y_test, y_pred_test)

f1_val = mi.show_f1(y_val, y_pred_val)
f1_test = mi.show_f1(y_test, y_pred_test)

table = PrettyTable()
table.field_names = ["Metric", "Validation result", "Test result"]

table.add_row(["Accuracy", accuracy_val, accuracy_test])
table.add_row(["Precision", precision_val, precision_test])
table.add_row(["Recall", recall_val, recall_test])
table.add_row(["F1 score", f1_val, f1_test])

print(table)


disp_cm_val = mi.show_confusion_matrix(y_val, y_pred_val, rf)
disp_cm_test = mi.show_confusion_matrix(y_test, y_pred_test, rf)

disp_cm_val.plot()
plt.title('Confusion Matrix for validation set')
plt.show()

disp_cm_test.plot()
plt.title('Confusion Matrix for test set')
plt.show()

lc_plt = mi.plot_learning_curve(rf, X_train_scaled, y_train)
lc_plt.show()

classes = np.unique(y_test)
y_test_bin = label_binarize(y_test, classes=classes)
roc_plt = mi.plot_roc_curve(rf, X_test_scaled, y_test_bin, classes)
roc_plt.show()