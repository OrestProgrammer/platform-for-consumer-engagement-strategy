import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier, VotingClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.svm import SVC
from sklearn.model_selection import train_test_split
from prettytable import PrettyTable
import src.models.python.databricks.common.metrics_info as mi
import src.models.python.databricks.common.data_manipulation as dm
import matplotlib.pyplot as plt
import pickle


input_df = pd.read_csv('/Users/orestchukla/Desktop/Універ/4 курс/Дипломна/SparkProject/data/gold/online_consumer_full_gold.csv')

processed_df = dm.data_preprocessing(input_df)

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

y_pred_val = ensemble.predict(X_val_scaled)
y_pred_test = ensemble.predict(X_test_scaled)

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

disp_cm_val = mi.show_confusion_matrix(y_val, y_pred_val, ensemble)
disp_cm_test = mi.show_confusion_matrix(y_test, y_pred_test, ensemble)

disp_cm_val.plot()
plt.title('Confusion Matrix for validation set')
plt.show()

disp_cm_test.plot()
plt.title('Confusion Matrix for test set')
plt.show()

lc_plt = mi.plot_learning_curve(ensemble, X_train_scaled, y_train)
lc_plt.show()