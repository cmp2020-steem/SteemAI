from SteemAI import DeepPreprocess as dp
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error

data = dp.get_data_regression(60)

X = data.drop('total_value', axis=1)
y = data['total_value']

X_train, X_test, y_train, y_test = train_test_split(X.values, y.values, test_size=0.25)

scaler = MinMaxScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

model = RandomForestRegressor(n_estimators=300, verbose=3)
model.fit(X_train, y_train)
pred = model.predict(X_test)

mse = mean_squared_error(y_test, pred)
print(mse)
rmse = mse**.5
print(rmse)

for x in range(len(y_test)):
    print(f'Predicted: {pred[x]} | Actual: {y_test[x]}')

print(model.score(X_test, y_test))
