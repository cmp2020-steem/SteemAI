from Downloading.SteemSQL import SSQL
import torch, numpy as np, pandas as pd, transformers
from SteemAI import DeepPreprocess as dp
from keras.preprocessing.sequence import pad_sequences
from sklearn.model_selection import train_test_split

max_length = 128

db = SSQL()

df = db.get_data('TrainingBlockchainData')
df = df[df['body_word_count'] >= 100]
print(len(df))
df = df.dropna()
print(len(df))

df['created'] = pd.to_datetime(df['created'])
df = dp.get_percentiles(data=df)

model = transformers.BertForSequenceClassification.from_pretrained("bert-base-uncased")

tokenizer = transformers.BertTokenizer.from_pretrained("bert-base-uncased")

input_ids = [tokenizer.encode(text, add_special_tokens=True) for text in df['body']]
input_ids = pad_sequences(input_ids, maxlen=max_length, dtype="long", truncating="post", padding="post")
input_ids = torch.tensor(input_ids)
labels = torch.tensor(df['percentile'].values)

train_inputs, validation_inputs, train_labels, validation_labels = train_test_split(input_ids, labels, test_size=0.1)
model.train()

loss_fn = torch.nn.BCELoss, optimizer = torch.optim.Adam(model.parameters(), lr=2e-5)

epochs = 25
for epoch in range(epochs):
    train_loss = 0
    for inputs, labels in zip(train_inputs, train_labels):
        optimizer.zero_grad()
        outputs = model(inputs.unsqueeze(0))
        loss = loss_fn(outputs.squeeze(), labels.float())
        loss.backward()
        optimizer.step()
        train_loss += loss.item()

validation_loss = 0
for inputs, labels in zip(validation_inputs, validation_labels):
    outputs = model(inputs.unsqueeze(0))
    loss = loss_fn(outputs.squeeze(), labels.float())
    validation_loss += loss.item()

print(validation_loss, train_loss)