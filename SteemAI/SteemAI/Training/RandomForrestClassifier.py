from SteemAI import DeepPreprocess as dp
import joblib
import pickle
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
import datetime
import os

def display(results):
    print(f'Best parameters are: {results.best_params_} | Score: {results.best_score_}')
    print("\n")
    return results.best_params_

def createWeekModels(date:str=None):
    if date == None:
        date = datetime.datetime.now().strftime('%m-%d-%Y')
    if not os.path.exists(f'{date}-models'):
        os.mkdir(f'{date}-models')
    df = dp.get_dataframe(include_zero=False, n_months=12, upper_dollar=1)
    for x in range(10, 0, -1):
        dollar = x * 10

        print(dollar)

        data = dp.get_data_classification(df, dollar)

        test_point = int(len(data) * 0.25)

        training_data = data.iloc[0:-test_point]
        test_data = data.iloc[-test_point:len(data)]
        print(len(test_data))

        X_train = training_data.drop(['total_value', 'total_value_bool'], axis=1).values
        X_test = test_data.drop(['total_value', 'total_value_bool'], axis=1).values
        y_train = training_data['total_value_bool'].values
        y_test = test_data['total_value_bool'].values

        model = RandomForestClassifier(n_estimators=250)

        model.fit(X_train, y_train)

        pred = model.predict(X_test) > 0.5

        print('TN FP')
        print('FN TP')
        print(confusion_matrix(y_test, pred))
        print(classification_report(y_test, pred))

        print(model.score(X_test, y_test))

        with open(f"./{date}-models/{date}-{dollar}dollarModel.pkl", 'wb') as f:
            pickle.dump(model, f)

if __name__ == '__main__':
    createWeekModels()