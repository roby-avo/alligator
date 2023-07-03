from sklearn.linear_model import LogisticRegression
from imblearn.over_sampling import SMOTE, ADASYN
from sklearn.metrics import classification_report
from sklearn.utils.class_weight import compute_class_weight

import pandas as pd
# first neural network with keras tutorial
from numpy import loadtxt
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, BatchNormalization
from sklearn.preprocessing import LabelEncoder
from keras.utils import np_utils
import numpy as np
import pandas as pd

path = "./data/ml_with_type/10/final_ml.csv"
train_fold = pd.read_csv(path)

X_train, y_train = train_fold.drop(["tableName", "target"], axis=1), train_fold["target"]
class_weights = compute_class_weight(
    class_weight = "balanced",
    classes = np.unique(y_train),
    y = y_train                                          
)
X_resampled, y_resampled = (X_train, y_train)
Y_train = np_utils.to_categorical(y_resampled, 2)
# load the dataset
# define the keras model
model = Sequential()
model.add(Dense(64, input_shape=(len(X_train.columns),), activation='relu'))
model.add(BatchNormalization())
model.add(Dense(128, activation='relu'))
model.add(BatchNormalization())
model.add(Dense(256, activation='relu'))
model.add(BatchNormalization())
model.add(Dense(128, activation='relu'))
model.add(BatchNormalization())
model.add(Dense(64, activation='relu'))
model.add(BatchNormalization())
model.add(Dense(2, activation='softmax'))
# compile the keras model
model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
# fit the keras model on the dataset
model.fit(X_resampled, Y_train, epochs=100, batch_size=1000000, class_weight={0:class_weights[0], 1:class_weights[1]})
# Test, Loss and accuracy
loss_and_metrics = model.evaluate(X_resampled, Y_train, batch_size=1000000)
print('Loss = ',loss_and_metrics[0])
print('Accuracy = ',loss_and_metrics[1])
predictions = model.predict(X_train, batch_size=1000000)
# Convert the predictions to binary labels
predictions = np.where(predictions > 0.5, 1, 0)

# Calculate the classification report
report = classification_report(Y_train, predictions)

# Print the report
print(report)
model.save("neural_network_temp.h5")
