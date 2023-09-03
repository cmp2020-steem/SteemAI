from keras.models import Model
from keras.layers import Input, Dense, Embedding, concatenate, Dropout, Flatten
from keras.preprocessing.text import Tokenizer
from keras.preprocessing.sequence import pad_sequences
from sklearn.preprocessing import MinMaxScaler
import numpy as np
import pandas as pd
import tensorflow as tf
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from keras.preprocessing.text import Tokenizer
from keras.preprocessing.sequence import pad_sequences
from SteemAI import DeepPreprocess as dp
from sklearn.metrics import classification_report, confusion_matrix
from keras.callbacks import EarlyStopping

# Load the data and split into X (features) and y (target)
df = dp.get_dataframe(include_zero=False, n_months=12, min_body_len=100, max_body_len=3000, upper_dollar=1, min='50_min')
df = dp.get_percentile(df, 65)
df = dp.preprocess_data(df)
print(len(df))
df = df[df['en'] == True]
print(len(df))
data, original_features = dp.get_training_dataframe_50_min(df, include_percentile=True, include_title=True)
columns = data.columns.tolist()

test_point = int(len(data) * 0.25)

training_data = data.iloc[0:-test_point]
test_data = data.iloc[-test_point:len(data)]
print(len(test_data))

# Preprocess title column
vectorizer = CountVectorizer(stop_words='english')
title_features = vectorizer.fit_transform(data['title'])

# Convert sparse matrix to numpy array
title_features = title_features.toarray()

data['title'] = title_features

test_point = int(len(data) * 0.25)

training_data = data.iloc[0:-test_point]
test_data = data.iloc[-test_point:len(data)]
print('made training_data')

X_train = training_data.drop('percentile', axis=1).values
X_test = test_data.drop('percentile', axis=1).values
y_train = training_data['percentile'].values
y_test = test_data['percentile'].values

# Train a Random Forest classifier
rfc = RandomForestClassifier(n_estimators=100, random_state=42)
rfc.fit(X_train, y_train)



pred = rfc.predict(X_test) > 0.5
print('TN FP')
print('FN TP')
print(confusion_matrix(y_true=y_test, y_pred=pred))
print(classification_report(y_true=y_test, y_pred=pred))

