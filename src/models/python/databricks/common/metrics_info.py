import numpy as np
import sklearn.metrics as metrics
from sklearn.model_selection import learning_curve
import matplotlib.pyplot as plt


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
    sizes, training_scores, validation_scores = learning_curve(model, X, y, cv=5,
                                                               train_sizes=np.linspace(0.1, 1, 10), scoring='accuracy')

    mean_training = np.mean(training_scores, axis=1)
    mean_validation = np.mean(validation_scores, axis=1)

    plt.figure(figsize=(10, 6))
    plt.plot(sizes, mean_training, '-o', color="r", label="Training score")
    plt.plot(sizes, mean_validation, '-o', color="g", label="Validation score")

    plt.title('Learning Curve')
    plt.xlabel('Size of training samples')
    plt.ylabel('Accuracy')
    plt.legend(loc="lower right")
    plt.grid(True)

    return plt