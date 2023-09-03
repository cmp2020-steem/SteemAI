import pandas as pd
from Downloading.SteemSQL import SSQL
from datetime import datetime, timedelta

def create_boolean_total_value(data, point):
    data['total_value_bool'] = data['total_value'] >= point
    data['total_value_bool'] = data['total_value_bool'].astype(int)
    return data

def get_percentile(data:pd.DataFrame, perc:int=None) -> pd.DataFrame:
    data['percentile'] = (data.groupby(data.created.dt.date)['total_value'].rank(pct=True, method='first') * 100).astype(int)
    data['percentile'] = data['percentile'].mask(data['percentile'] < perc, 0)
    data['percentile'] = data['percentile'].mask(data['percentile'] >= perc, 1)
    return data

def get_percentiles(data:pd.DataFrame) -> pd.DataFrame:
    data['percentile'] = (data.groupby(data.created.dt.date)['total_value'].rank(pct=True, method='first') * 100).astype(int)
    return data

def get_post_tag_data(post, df):
    for x in range(1,6):
        temp = df[df[f'tag{x}']!='None']
        temp = temp[temp[f'tag{x}'] == post[f'tag{x}']]
        post.loc[f'tag{x}_mean_total_value'] = temp['total_value'].mean()
        post.loc[f'tag{x}_median_total_value'] = temp['total_value'].median()
        post.loc[f'tag{x}_total_posts'] = len(temp)
        post.loc[f'tag{x}_max_total_value'] = temp['total_value'].max()
        post.loc[f'tag{x}_min_total_value'] = temp['total_value'].min()
        post = post.fillna(0)
    return post

def get_tag_data(df):
    for x in range(1,6):
        temp = df[df[f'tag{x}']!='None']
        df[f'tag{x}_mean_total_value'] = temp.groupby(f'tag{x}')['total_value'].transform('mean')
        df[f'tag{x}_median_total_value'] = temp.groupby(f'tag{x}')['total_value'].transform('median')
        df[f'tag{x}_max_total_value'] = temp.groupby(f'tag{x}')['total_value'].transform('max')
        df[f'tag{x}_min_total_value'] = temp.groupby(f'tag{x}')['total_value'].transform('min')
        df[f'tag{x}_total_posts'] = temp.groupby(f'tag{x}')[f'tag{x}'].transform('count')
        df[f'tag{x}_mean_total_value'] = df[f'tag{x}_mean_total_value'].fillna(0)
        df[f'tag{x}_median_total_value'] = df[f'tag{x}_median_total_value'].fillna(0)
        df[f'tag{x}_max_total_value'] = df[f'tag{x}_max_total_value'].fillna(0)
        df[f'tag{x}_min_total_value'] = df[f'tag{x}_min_total_value'].fillna(0)
        df[f'tag{x}_total_posts'] = df[f'tag{x}_total_posts'].fillna(0)
    return df

def get_language_data(df):
    df['primary_language_mean_total_value'] = df.groupby(['primary_language'])['total_value'].transform('mean')
    df['primary_language_median_total_value'] = df.groupby(['primary_language'])['total_value'].transform('median')
    df['primary_language_max_total_value'] = df.groupby(['primary_language'])['total_value'].transform('max')
    df['primary_language_min_total_value'] = df.groupby(['primary_language'])['total_value'].transform('min')
    df['primary_language_total_posts'] = df.groupby(['primary_language'])['total_value'].transform('count')
    return df

def get_post_language_data(post, df):
    temp = df[df['primary_language'] == post['primary_language']]
    post.loc['primary_language_mean_total_value'] = temp['total_value'].mean()
    post.loc['primary_language_median_total_value'] = temp['total_value'].median()
    post.loc['primary_language_max_total_value'] = temp['total_value'].max()
    post.loc['primary_language_min_total_value'] = temp['total_value'].min()
    post.loc['primary_language_total_posts'] = len(temp)
    return post

def get_rep_data(df):
    df['rep_int'] = df['author_reputation'].astype(int)
    df['author_rep_mean_total_value'] = df.groupby(['rep_int'])['total_value'].transform('mean')
    df['author_rep_median_total_value'] = df.groupby(['rep_int'])['total_value'].transform('median')
    df['author_rep_max_total_value'] = df.groupby(['rep_int'])['total_value'].transform('max')
    df['author_rep_min_total_value'] = df.groupby(['rep_int'])['total_value'].transform('min')
    df['author_rep_total_posts'] = df.groupby(['rep_int'])['rep_int'].transform('count')
    df = df.drop('rep_int', axis=1)
    return df

def get_post_rep_data(post, df):
    df['rep_int'] = df['author_reputation'].astype(int)
    temp = df[df['rep_int'] == int(post['author_reputation'])]
    post.loc['author_rep_mean_total_value'] = temp['total_value'].mean()
    post.loc['author_rep_median_total_value'] = temp['total_value'].median()
    post.loc['author_rep_max_total_value'] = temp['total_value'].max()
    post.loc['author_rep_min_total_value'] = temp['total_value'].min()
    post.loc['author_rep_total_posts'] = len(temp)
    post = post.fillna(0)
    return post

def get_follower_data(df):
    df['author_followers_100'] =  (df['author_followers'] / 100).astype(int) * 100
    df['author_followers_100_avg_total_value'] = df.groupby(['author_followers_100'])['total_value'].transform('mean')
    df['author_followers_100_median_total_value'] = df.groupby(['author_followers_100'])['total_value'].transform('median')
    df['author_followers_100_max_total_value'] = df.groupby(['author_followers_100'])['total_value'].transform(
        'max')
    df['author_followers_100_min_total_value'] = df.groupby(['author_followers_100'])['total_value'].transform(
        'min')
    df = df.drop('author_followers_100', axis=1)
    return df

def get_post_follower_data(post, df):
    df['author_followers_100'] =  (df['author_followers'] / 100).astype(int) * 100
    temp = df[df['author_followers_100'] == int(post['author_followers'] / 100) * 100]
    post.loc['author_followers_100_avg_total_value'] = temp['total_value'].mean()
    post.loc['author_followers_100_median_total_value'] = temp['total_value'].median()
    post.loc['author_followers_100_max_total_value'] = temp['total_value'].max()
    post.loc['author_followers_100_min_total_value'] = temp['total_value'].min()
    return post

def initial_preprocess_reg(data):
    data = get_tag_data(data)
    data = get_rep_data(data)
    data = data[data['1_hour_value'] <= 1]
    data = data[data['1_hour_value'] > 0]
    drops = ['title', 'link', 'spelling_errors', 'created', 'total_votes', 'total_comments',
             'author_value', 'curation_value', 'beneficiary_value', 'block', 'tag1', 'tag2', 'tag3', 'tag4', 'tag5', 'author']
    for x in drops:
        data = data.drop(x, axis=1)
    dummy1 = pd.get_dummies(data['primary_language'], drop_first=True)
    data = pd.concat([data, dummy1], axis=1).drop('primary_language', axis=1)
    data = data.dropna()
    return data

def preprocess_data(data):
    data = data.drop('id', axis=1)
    data = get_tag_data(data)
    data = get_rep_data(data)
    data = get_language_data(data)
    data = get_follower_data(data)

    dummy1 = pd.get_dummies(data['primary_language'], drop_first=True)
    data = pd.concat([data, dummy1], axis=1).drop('primary_language', axis=1)
    return data

def preprocess_post(post:pd.Series, data):
    post = get_post_tag_data(post, data)
    post = get_post_rep_data(post, data)
    post = get_post_language_data(post, data)
    post = get_post_follower_data(post, data)
    dummy1 = pd.get_dummies(data['primary_language'], drop_first=True)
    for col in dummy1.columns:
        if col == post['primary_language']:
            post[col] = 1
        else:
            post[col] = 0
    post = post.drop('primary_language')
    return post.fillna(0)

def get_data_regression(time):
    db = SSQL()
    blockchain_data = db.get_data('TrainingBlockchainData')
    steemworld_data = db.get_data('SteemWorldData')
    data = pd.merge(blockchain_data, steemworld_data, on='id')
    data = preprocess_data(data, time)
    return data

def get_dataframe(n_months:int=None, include_zero:bool=False, min_body_len:int=100, max_body_len:int=3000, upper_dollar:float=1.0, min:str='60_min', blacklist_authors=False, blacklist_file=''):
    if blacklist_authors:
        with open(blacklist_file, 'r') as f:
            blacklisted_authors = [line.rstrip('\n') for line in f]
            print(blacklisted_authors)
    else:
        blacklisted_authors = []
    db = SSQL()
    blockchain_data = db.get_data('TrainingBlockchainData').drop('inserted', axis=1)
    steemworld_data = db.get_data('SteemWorldData').drop('inserted', axis=1)
    data = pd.merge(blockchain_data, steemworld_data, on='id')
    data = data[~data['author'].isin(blacklisted_authors)]
    data = data[data[f'{min}_value'] <= upper_dollar]
    data = data[data['body_word_count']>= min_body_len]
    data = data[data['body_word_count'] <= max_body_len]
    if not include_zero:
        data = data[data[f'{min}_value'] != 0]
    if n_months == None:
        return data
    else:
        earliest_date = datetime.now() - timedelta(days=n_months*30)
        data['created'] = pd.to_datetime(data['created'])
        data = data[data['created'] >= earliest_date]
        return data

def get_data_classification(df, point):
    df = preprocess_data(df)
    final_df = create_boolean_total_value(df, point)
    return final_df

def get_data_percentile_classification(df, perc):
    df = get_percentile(df, perc)
    final_df = preprocess_data(df)
    return final_df

def get_post_classification(post, df, dataframe_path, calculated_path, language_path):
    post = preprocess_post(post, df)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    features = get_final_features(dataframe_features_path=dataframe_path, calculated_features_path=calculated_path, language_features_path=language_path)
    for item in features:
        if item not in post.index:
            post[item] = 0
    post = post[features]
    post = post.reindex(index=features)

    return post.values.reshape(1,-1)


def get_final_features(dataframe_features_path='', calculated_features_path='../', language_features_path='../'):
    dataframe_features = open(f'{dataframe_features_path}/dataframe_features.txt').read().splitlines()
    calculated_features = open(f'{calculated_features_path}/calculated_features.txt').read().splitlines()
    languages = open(f'{language_features_path}/language_codes.txt').read().splitlines()
    features = dataframe_features + calculated_features + languages
    return features

def get_training_dataframe_60_min(df, include_percentile=False):
    original_features = get_final_features()
    features = original_features.copy()
    if include_percentile:
        features.append('percentile')
    df = df[features]
    df = df.dropna()
    return df, original_features


def get_training_dataframe_50_min(df, include_percentile=False, include_title=False, include_author=False):
    original_features = get_final_features(
        dataframe_features_path='../FeatureLists/Feb11',
        calculated_features_path='../',
        language_features_path='../'
    )
    features = original_features.copy()
    if include_percentile:
        features.append('percentile')
    if include_title:
        features.append('title')
    if include_author:
        features.append('author')
    for feature in features:
        if feature not in df.columns:
            df[feature] = 0
    df = df[features]
    df = df.dropna()
    return df, original_features