import pickle
from steem import Steem
from VotingBot import VotingBot
from datetime import datetime, timedelta
from SteemAI import DeepPreprocess as dp
import warnings
import time
from Downloading.SteemPostData import get_live_post_data, get_post_current_value
from Downloading.SteemSQL import SSQL
from VotingBotDir.Streaming.getPosts import get_articles_in_time_frame
import pandas as pd
import os
from random import randint
import math
from getpass import getpass

percentile_path = '../../SteemAI/percentiles_to_predict.txt'
with open(percentile_path, 'r') as f:
    PERCENTILES = eval(f.read())

s = Steem()

def get_models():
    models = {}
    for x in range(1, 11):
        with open(f'../../SteemAI/Models/PercentileModels/model{x}.pkl', 'rb') as f:
            models[f'Model{x}'] = pickle.load(f)
    return models

streamed_posts = []

def calculate_weight(voting_power, score):
    if voting_power >= 60:
        # Calculate weight based on a linear scale between 20% and 80%
        weight_range = 80 - 20
        weight = 20 + (weight_range * (score - 1) / 54)
    else:
        # Calculate weight based on a linear scale between 10% and 60%
        weight_range = 60 - 10
        weight = 10 + (weight_range * (score - 1) / 54)

    # Adjust weight based on voting power
    weight = weight * (voting_power / 100)

    # Ensure weight is within the 1-100 range
    return max(1, min(100, weight))

def eval_post(post, models):
    predictions = {}
    for key in models.keys():
        predictions[key] = models[key].predict(post) > 0.5

    score = 0
    highest_percentile = 0
    confusion = 0
    num_of_pot_models = 0
    for x in range(10, 0, -1):
        if predictions[f'Model{x}']:
            score += x
            if PERCENTILES[x-1] > highest_percentile:
                highest_percentile = PERCENTILES[x-1]
                num_of_pot_models = x
            if PERCENTILES[x-1] < highest_percentile:
                confusion += 1
    if num_of_pot_models != 0:
        confusion_ratio = format(confusion / num_of_pot_models, ".2f")
    else:
        confusion_ratio = None
    return score, highest_percentile, confusion, confusion_ratio, predictions

def roll(odds):
    return randint(0, odds) == 0


def sigmoid_choice(input_value):
    k = 0.1  # You can adjust this constant to control the steepness of the curve
    x0 = 50  # Midpoint of the curve

    odds = 1 / (1 + math.exp(-k * (input_value - x0)))
    return odds

def decide_to_vote_sigmoid_scaled(score, vp, val):
    if val > 1:
        print (f"Value ({val}) too high! Score of {score}. Returning!")
        return -1
    weight = int(calculate_weight(vp, score))
    base = round(((1 - sigmoid_choice(vp)) + .5) * 3) + 2
    sc = int(score/10) - 1
    if sc < 0:
        sc = 0
    roll_input = base - sc
    print(f'vp: {vp} score: {score} odds: 1/{roll_input} weight: {weight}%')
    if roll(roll_input):
        return weight
    else:
        return 0

def decide_to_vote(score, vp, val):
    weight = 0
    offset = 5
    if vp <= 70:
        offset = 0
    elif vp >= 80:
        offset = offset + int(vp - 85)
        print(f'Offset of {offset}')

    # Added Jan 7. to block voting on posts over $1.
    if val > 1:
        print (f"Value ({val}) too high! Score of {score}. Returning!")
        return -1
    # End Jan 7. change.

    if score > 45:
        return score + offset
    elif score > 36:
        return score + offset
    elif score > 28:
        return score + offset
    elif score > 21:
        if roll(2):
            return score + offset
    elif score > 15:
        if roll(2):
            return score + offset
    elif score > 10:
        if roll(3):
            return score + offset
    elif score > 6 and val <= 0.5:
        if roll(3):
            return score + offset
    elif score > 3 and val <= 0.45:
        if roll(4):
            return score + offset
    elif score > 1 and val <= 0.4:
        if roll(5):
            return score + offset
    elif score == 1 and val <=0.35:
        if roll(6):
            return score + offset
    else:
        ## VP kluge, Dec. 1
        if roll(7):
            return score + offset
        else:
            return 0
    return weight

def voting_bot(blacklist_authors=False):
    if blacklist_authors:
        with open('../../SteemAI/blacklisted_authors.txt', 'r') as f:
            blacklisted_authors = blacklisted_authors = [line.rstrip('\n') for line in f]
    else:
        blacklisted_authors = []
    warnings.simplefilter('ignore')
    cub1_posting_key = input("What is cub1's key?")
    cmp2020_posting_key = input("What is cmp2020's key?")
    accounts = [VotingBot('cmp2020', cmp2020_posting_key), VotingBot('cub1', cub1_posting_key)]
    db = SSQL()
    df = dp.get_dataframe(include_zero=False, n_months=12, upper_dollar=1, blacklist_authors=blacklist_authors)
    last_update = datetime.now()
    models = get_models()
    cub1_votes=[]
    cub1_total_votes=0
    cmp2020_votes=[]
    cmp2020_total_votes=0
    last_author = None
    last_permlink = None

    while True:
        if last_update <= datetime.now() - timedelta(minutes=20):
            while True:
                path = '../Models/ChangingModels'
                if not os.path.exists(path):
                    models = get_models()
                    print('updated models')
                    df = dp.get_dataframe(include_zero=False, n_months=12, upper_dollar=1)
                    last_update = datetime.now()
                    break
                else:
                    time.sleep(5)
        try:
            articles = get_articles_in_time_frame(50, 60, last_author=last_author, last_permlink=last_permlink, blacklisted_authors=blacklisted_authors)
        except Exception as e:
            print('error here')
            print(e)
            articles = []
        if len(articles) > 0:
            for article in articles:
                try:
                    initial_post = get_live_post_data(article, time=50)
                    initial_post = pd.Series(initial_post)
                    post = dp.get_post_classification(initial_post, df,
                                                      dataframe_path='../../SteemAI/FeatureLists/Feb11',
                                                      calculated_path='../../SteemAI',
                                                      language_path='../../SteemAI'
                                                      )
                    score, highest_percentile, confusion, confusion_ratio, model_pred = eval_post(post, models)
                    if score == 0:
                        print('score of 0')
                    if score > 0:
                        for x in range(len(accounts)):
                            account = accounts[x]
                            voting_power = account.get_current_voting_power()
                            current_value = get_post_current_value(initial_post['author'], initial_post['permlink'])
                            weight = decide_to_vote_sigmoid_scaled(score, voting_power, current_value)
                            # Debugging - January 8
                            # End debugging
                            if weight > 0 and not account.get_post_voted_on(initial_post['author'], initial_post['permlink']):
                                account.vote(initial_post, weight)
                                vote = {}
                                vote['id'] = initial_post['id']
                                vote['author'] = initial_post['author']
                                vote['permlink'] = initial_post['permlink']
                                vote['vote_time'] = datetime.now()
                                vote['val_at_vote'] = current_value
                                vote['highest_percentile'] = highest_percentile
                                vote['confusion'] = confusion
                                vote['confusion_ratio'] = confusion_ratio
                                vote['score'] = score
                                vote['weight'] = weight
                                vote['voting_power'] = voting_power
                                vote['created'] = initial_post['created']
                                vote['payout_date'] = vote['created'] + timedelta(days=7)

                                for key in model_pred.keys():
                                    vote[key] = int(model_pred[key])
                                if x == 1:
                                    cub1_votes.append(vote)
                                    cub1_total_votes += 1
                                    print(f'Cub1 Voted! Score: {score}/55 | Highest_Percentile: {highest_percentile} | Voting Power: {voting_power}% Votes: {cub1_total_votes}')
                                elif x == 0:
                                    cmp2020_votes.append(vote)
                                    cmp2020_total_votes += 1
                                    print(f'Cmp2020 Voted! Score: {score}/55 | Highest_Percentile: {highest_percentile} | Weight: {weight}% | Voting Power: {voting_power}% | Votes: {cmp2020_total_votes}')
                                if len(cub1_votes) >= 1:
                                    db.insert_training_articles('cub1_votes', cub1_votes)
                                    cub1_votes = []
                                if len(cmp2020_votes) >= 1:
                                    db.insert_training_articles('cmp2020_votes', cmp2020_votes)
                                    cmp2020_votes = []
                            elif weight == 0:
                                vote = {}
                                vote['id'] = initial_post['id']
                                vote['author'] = initial_post['author']
                                vote['permlink'] = initial_post['permlink']
                                vote['vote_time'] = datetime.now()
                                vote['val_at_vote'] = current_value
                                vote['highest_percentile'] = highest_percentile
                                vote['score'] = score
                                vote['weight'] = weight
                                vote['voting_power'] = voting_power
                                vote['created'] = initial_post['created']
                                vote['payout_date'] = vote['created'] + timedelta(days=7)
                                if x == 1:
                                    cub1_votes.append(vote)

                except Exception as e:
                    print('error')
                    print(e)
                last_author = articles[-1]['author']
                last_permlink = articles[-1]['permlink']
        else:
            print('sleeping')
            time.sleep(180)

if __name__ == '__main__':
    voting_bot()
"""What is cub1's key?5HpgXQogYbcFvvYCMPUsdzkMWALS2DnDr5yUv6zUhKvfCZa2Ga2
What is cmp2020's key?5JeKaAJTPBZWoyDf5H8ik1uXXX3QenVTvV3gqQr4uk4C3CRcnET"""