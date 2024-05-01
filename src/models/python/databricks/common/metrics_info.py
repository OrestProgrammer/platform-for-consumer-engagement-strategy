# Databricks notebook source
import numpy as np
import sklearn.metrics as metrics
from sklearn.model_selection import learning_curve
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import ExtraTreesClassifier


def show_accuracy(y, y_pred):
    return round(metrics.accuracy_score(y, y_pred), 4)


def show_precision(y, y_pred):
    return round(metrics.precision_score(y, y_pred, average='weighted'), 4)


def show_recall(y, y_pred):
    return round(metrics.recall_score(y, y_pred, average='weighted'), 4)


def show_f1(y, y_pred):
    return round(metrics.f1_score(y, y_pred, average='weighted'), 4)


def show_confusion_matrix(y, y_pred, model):
    cm = metrics.confusion_matrix(y, y_pred)
    disp = metrics.ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=model.classes_)

    return disp


def plot_learning_curve(model, X, y):
    sizes, training_scores, testing_scores = learning_curve(model, X, y, cv=5,
                                                               train_sizes=np.linspace(0.1, 1, 10), scoring='accuracy')

    mean_training = np.mean(training_scores, axis=1)
    mean_testing = np.mean(testing_scores, axis=1)

    plt.figure(figsize=(10, 6))
    plt.plot(sizes, mean_training, '-o', color="r", label="Training score")
    plt.plot(sizes, mean_testing, '-o', color="g", label="Testing score")

    plt.title('Learning Curve')
    plt.xlabel('Size of training samples')
    plt.ylabel('Accuracy')
    plt.legend(loc="lower right")
    plt.grid(True)

    return plt


def plot_corr_matrix(df):
    corr_matrix = df.corr().abs()

    plt.figure(figsize=(50, 50))
    sns.heatmap(corr_matrix, annot=True, cmap="Reds", linewidths=1)

    plt.title("Correlation Matrix")

    return plt


def check_importances(df, target):
    X = df.drop(target, axis=1)
    y = df[target]
    model = ExtraTreesClassifier()
    model.fit(X, y)
    feature_importances = model.feature_importances_
    ind = np.argsort(feature_importances)[::-1]

    for i in range(X.shape[1]):
        print("Feature: " + X.columns[ind[i]] + ". Importance: " + str(feature_importances[ind[i]]))

    features = [X.columns[i] for i in ind]
    importances = feature_importances[ind]

    plt.figure(figsize=(10, 6))
    plt.barh(range(len(features)), importances, align='center', color='green')
    plt.yticks(range(len(features)), features)
    plt.xlabel('Importance')
    plt.ylabel('Feature')
    plt.title('Feature Importance')
    plt.show()
