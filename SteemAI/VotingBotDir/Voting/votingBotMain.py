import pickle
from steem import Steem
from VotingBot import VotingBot
from datetime import datetime, timedelta
from SteemAI import DeepPreprocess as dp
import warnings
import time
from VotingBotDir.Streaming.stream import read_live_file_ids, remove_live_file_id
from Downloading.SteemSQL import SSQL
from random import randint, random

s = Steem()

def get_models():
    models = {}
    for x in range(1, 11):
        with open(f'../../SteemAI/Models/PercentileModels/model{x}.pkl', 'rb') as f:
            models[f'Model{x}'] = pickle.load(f)
    return models

streamed_posts = []

def eval_post(post, models):
    predictions = {}
    for key in models.keys():
        predictions[key] = models[key].predict(post) > 0.5

    score = 0
    for x in range(10, 0, -1):
        if predictions[f'Model{x}']:
            score += x
    return score, predictions

def decide_to_vote(score, vp):
    weight = 0
    offset = 15
    if score > 45:
        return score + offset
    elif score > 36:
        if vp >= 72:
            return score + offset
    elif score > 28:
        if vp >= 75:
            return score + offset
    elif score > 21:
        if vp >= 78:
            return score + offset
    elif score > 15:
        if vp >= 80:
            return score + offset
    elif score > 10:
        if vp >= 83:
            return score + offset
    elif score > 6:
        if vp >= 84:
            return score + offset
    elif score > 3:
        if vp >= 85:
            return score + offset
    elif score > 1:
        if vp >= 90:
            return score + offset
    elif score == 1:
        if vp >= 95:
            return score + offset
    else:
        ## VP kluge, Dec. 1
        if ( random.randint (0,10) == 0):
            return score + offset
        else:
            return 0
    return weight

def voting_bot():
    warnings.simplefilter('ignore')
    posting_key = input('What is the account posting key?: ')
    account = VotingBot('cub1', posting_key)#remove later
    db = SSQL()
    df = dp.get_dataframe(include_zero=False, n_months=12, upper_dollar=1)
    last_update = datetime.now()
    models = get_models()
    total_votes = 0
    votes=[]

    while True:
        if last_update <= datetime.now() - timedelta(minutes=20):
            models = get_models()
            print('updated models')
            df = dp.get_dataframe(include_zero=False, n_months=12, upper_dollar=1)
            last_update = datetime.now()
        live_post_ids = read_live_file_ids()
        if len(live_post_ids) > 0:
            live_posts = db.get_live_posts(live_post_ids)
            for x in range(len(live_posts)):
                initial_post = live_posts.iloc[x]
                post = dp.get_post_classification(initial_post, df)
                score, model_pred = eval_post(post, models)
                if score == 0:
                    print('score of 0')
                if score > 0:
                    print('Predictions: ', model_pred)
                    voting_power = account.get_current_voting_power()
                    weight = decide_to_vote(score, voting_power)
                    if weight > 0:
                        account.vote(initial_post, weight)
                        vote = {}
                        vote['id'] = initial_post['id']
                        vote['author'] = initial_post['author']
                        vote['permlink'] = initial_post['permlink']
                        vote['vote_time'] = datetime.now()
                        vote['val_at_vote'] = initial_post['60_min_value']
                        vote['score'] = score
                        vote['weight'] = weight
                        vote['voting_power'] = voting_power

                        for key in model_pred.keys():
                            vote[key] = int(model_pred[key])
                        try:
                            votes.append(vote)
                        except:
                            votes=[]
                        total_votes += 1
                        print(f"Score: {score}/55 Weight: {weight}% Votes: {total_votes}")
                        if len(votes) >= 1:
                            db.insert_training_articles('cub1_votes', votes)
                            votes = []
            for id in live_post_ids:
                remove_live_file_id(id)
                db.remove_live_post(id)
        else:
            time.sleep(60)

if __name__ == '__main__':
    voting_bot()