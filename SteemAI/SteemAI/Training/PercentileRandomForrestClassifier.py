import sys
from SteemAI import DeepPreprocess as dp
import pickle
from sklearn.metrics import classification_report, confusion_matrix
from Downloading.SteemSQL import SSQL
from sklearn.ensemble import RandomForestClassifier
import shutil
import datetime
from time import time, sleep
import datetime as dt
import os
percentile_path = '../percentiles_to_predict.txt'

def daily_train():
    db = SSQL()
    with open(percentile_path, 'r') as f:
        percs = eval(f.read())

    model_data = {'date_trained':datetime.datetime.now()}
    count = 0
    path = '../ModelsNotLive/PercentileModels'
    if not os.path.exists(path):
        os.mkdir(path)
    for perc in percs:
        print(perc)
        count += 1
        model_data[f'model{count}_percentile'] = perc

        df = dp.get_dataframe(include_zero=False, n_months=12, min_body_len=100, max_body_len=3000, upper_dollar=1, min='50_min', blacklist_authors=True, blacklist_file='../../SteemAI/blacklisted_authors.txt')
        df = dp.get_percentile(df, perc)
        df = dp.preprocess_data(df)
        data, original_features = dp.get_training_dataframe_50_min(df, include_percentile=True)
        columns = data.columns.tolist()

        test_point = int(len(data) * 0.25)

        training_data = data.iloc[0:-test_point]
        test_data = data.iloc[-test_point:len(data)]
        print(len(test_data))

        X_train = training_data.drop('percentile', axis=1).values
        X_test = test_data.drop('percentile', axis=1).values
        y_train = training_data['percentile'].values
        y_test = test_data['percentile'].values
        clf = RandomForestClassifier()

        model = RandomForestClassifier(n_estimators=300, verbose=0)

        model.fit(X_train, y_train)

        pred = model.predict(X_test) > 0.5

        cm = confusion_matrix(y_test, pred)
        model_data[f'model{count}_tn'] = cm[0][0]
        model_data[f'model{count}_fn'] = cm[1][0]
        model_data[f'model{count}_tp'] = cm[1][1]
        model_data[f'model{count}_fp'] = cm[0][1]
        print(f'TN: {cm[0][0]}')
        print(f'FN: {cm[1][0]}')
        print(f'TP: {cm[1][1]}')
        print(f'FP: {cm[0][1]}')
        print(classification_report(y_test, pred))

        print(model.score(X_test, y_test))
        print('----------------------------------------------------------------------------------------')

        with open(f"../ModelsNotLive/PercentileModels/model{count}.pkl", 'wb+') as f:
            pickle.dump(model, f)

    path = '../Models/ChangingModels'
    os.mkdir(path)
    date = datetime.datetime.now()
    path = f'../Models/PercentileModels {date.month}-{date.day}-{date.year}'
    os.mkdir(path)
    destination = path
    f = '../Models/PercentileModels'
    shutil.move(f, destination)
    destination = f
    f = '../ModelsNotLive/PercentileModels'
    shutil.move(f, destination)
    path = '../Models/ChangingModels'
    os.rmdir(path)
    path = f'../Models/PercentileModels {date.month}-{date.day}-{date.year}'
    shutil.rmtree(path)
    db.insert_training_articles('ModelStats', [model_data], return_n=False)
    print('Succesfully inserted data for today!')


if __name__ == '__main__':
    while True:
        now = dt.datetime.now().time()
        goal = dt.time(hour=2, minute=30)
        if now.hour == goal.hour and now.minute == goal.minute:
            print('Training!')
            daily_train()
        else:
            now_datetime = datetime.datetime.combine(datetime.date.today(), now)
            target_datetime = datetime.datetime.combine(datetime.date.today(), goal)

            # Calculate the difference between the two times
            time_diff = target_datetime - now_datetime
            print(f'waiting {time_diff.seconds} seconds')
            sleep(time_diff.seconds)