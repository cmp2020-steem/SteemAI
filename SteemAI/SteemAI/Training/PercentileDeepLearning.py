import pandas as pd
import tensorflow as tf
from keras.preprocessing.text import Tokenizer
from keras.preprocessing.sequence import pad_sequences
from SteemAI import DeepPreprocess as dp
from sklearn.metrics import classification_report, confusion_matrix
from keras.callbacks import EarlyStopping
from keras.layers import Dropout

# Load the data into a pandas DataFrame
df = dp.get_dataframe(12, min='50_min')

df = dp.get_percentile(df, 65)
df = df[df['primary_language']=='en']

test_point = int(len(df) * 0.25)
train_df = df.iloc[0:-test_point]
test_df = df.iloc[-test_point:len(df)]


# Define the maximum number of words to use in the tokenizer
max_words = 1000

# Create a tokenizer to convert the text to sequences of integers
tokenizer = Tokenizer(num_words=max_words, oov_token="<OOV>")
tokenizer.fit_on_texts(train_df['title'])

# Convert the text to sequences of integers and pad them to a fixed length
sequences = tokenizer.texts_to_sequences(train_df['title'])
padded_sequences = pad_sequences(sequences, padding='post', truncating='post', maxlen=1000)

# Define the model architecture
model = tf.keras.Sequential([
    tf.keras.layers.Embedding(max_words, 32, input_length=1000),
    tf.keras.layers.Conv1D(64, 5, activation='relu'),
    tf.keras.layers.Dropout(0.5),
    tf.keras.layers.GlobalMaxPooling1D(),
    tf.keras.layers.Dense(64, activation='relu'),
    tf.keras.layers.Dropout(0.5),
    tf.keras.layers.Dense(64, activation='relu'),
    tf.keras.layers.Dropout(0.5),
    tf.keras.layers.Dense(1, activation='sigmoid')
])

loss_early = EarlyStopping(monitor='val_loss', restore_best_weights=True, patience=5)

# Compile the model
model.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy'])

# Train the model
model.fit(padded_sequences, train_df['percentile'], epochs=3, validation_split=0.25, callbacks=[loss_early])

# Evaluate the model on the test data
test_sequences = tokenizer.texts_to_sequences(test_df['title'])
padded_test_sequences = pad_sequences(test_sequences, padding='post', truncating='post', maxlen=1000)
test_loss, test_acc = model.evaluate(padded_test_sequences, test_df['percentile'])
print('Test accuracy:', test_acc)

pred = model.predict(padded_test_sequences) > 0.5
print('TN FP')
print('FN TP')
print(confusion_matrix(y_true=test_df['percentile'], y_pred=pred))
print(classification_report(y_true=test_df['percentile'], y_pred=pred))

print(model.score(padded_test_sequences, test_df['percentile']))

