import pandas as pd
from steem import Steem
from steem.account import Account
from Downloading.SteemSQL import SSQL
from Downloading.AccountInfo import getPrice
from Downloading.SteemPostData import get_post_historic_value, get_voter_value
from Downloading import AccountInfo as acc
from steembase.exceptions import RPCError
from SteemAI.DeepPreprocess import get_dataframe, get_percentiles
import datetime
from Downloading.SteemPostData import get_post_historic_value

s = Steem()
db = SSQL()

def get_payout_transaction(post):
    date = datetime.datetime.strptime(post['created'], '%Y-%m-%dT%H:%M:%S') + datetime.timedelta(days=7)
    account = Account(post['author'])
    rewards = list(account.get_account_history(index=-1, limit=0))
    highest_index = rewards[0]['index']
    lowest_index = 0
    running = True
    loop_count = 0
    while running:
        loop_count += 1
        mid_index = (highest_index + lowest_index) // 2
        mid_reward = list(account.get_account_history(index=mid_index, limit=0))[0]
        mid_time_stamp = datetime.datetime.strptime(mid_reward['timestamp'], '%Y-%m-%dT%H:%M:%S')
        if mid_time_stamp < date:
            lowest_index = mid_index
        elif mid_time_stamp > date:
            highest_index = mid_index
        if mid_time_stamp == date:
            return mid_reward

        if highest_index - lowest_index <= 1:
            return None
        if loop_count > 50:
            print("Looped more than 50")
            return None
    """curation_rewards = list(account.get_account_history(index=highest_index, limit=100))
    for reward in curation_rewards:
        time_stamp = datetime.datetime.strptime(reward['timestamp'], '%Y-%m-%dT%H:%M:%S')
        if time_stamp == date:
            return reward"""

def get_curator_post_stats(curator, post, steem_price, block_num=None):
    curator_rshares = 0
    total_rshares = 0
    for vote in post['active_votes']:
        total_rshares += int(vote['rshares'])
        if vote['voter'] == curator:
            curator_rshares = int(vote['rshares'])
    rshare_ratio = curator_rshares / total_rshares
    total_value = get_post_historic_value(post, 10080)
    if block_num == None:
        block_num = get_payout_transaction(post)['block']

    block = s.get_ops_in_block(block_num, True)
    total_vests = 0
    curator_vests = 0
    for x in range(len(block)):
        operation = block[x]['op']
        if operation[0] == 'curation_reward' and operation[1]['comment_author'] == post['author'] and operation[1]['comment_permlink'] == post['permlink']:
            total_vests += float(operation[1]['reward'].replace(' VESTS', ''))
            if operation[1]['curator'] == curator:
                curator_vests = float(operation[1]['reward'].replace(' VESTS', ''))
    reward_ratio = curator_vests/total_vests


    if rshare_ratio != 0:
        efficiency = int((reward_ratio / rshare_ratio) * 100)
    else:
        efficiency = 0
    total_curator_value = total_value / 2
    curator_reward_value = (curator_vests / total_vests) * total_curator_value
    curator_reward_steem = curator_reward_value / steem_price
    return {'id':int(post['id']),'efficiency':efficiency, 'val_at_payout':total_value, 'cub1_rewards_steem':curator_reward_steem, 'cub1_rewards_dollars':curator_reward_value}

def get_unrecorded_rewards():
    votes = db.get_data('cub1_votes')
    votes['vote_time'] = pd.to_datetime(votes['vote_time'])
    cuttoff = datetime.datetime.now() - datetime.timedelta(days=6, hours=23)
    votes = votes[votes['vote_time'] <= cuttoff]
    min_time = datetime.datetime.now() - datetime.timedelta(days=21)
    votes = votes[votes['vote_time'] >= min_time]
    votes = votes[votes['efficiency'].isna()]

    updated = []
    total_efficiency = 0
    count = 0
    dates = {}
    for x in range(len(votes)-1, 0, -1):
        vote = votes.iloc[x]
        post = s.get_content(vote['author'], vote['permlink'])
        payout_date = datetime.datetime.strptime(post['created'], '%Y-%m-%dT%H:%M:%S') + datetime.timedelta(days=7)
        date = datetime.datetime(month=payout_date.month, day=payout_date.day, year=payout_date.year)
        str_date = date.strftime('%d-%m-%Y')
        if str_date not in dates.keys():
            price = db.get_price(date)
            if price == None:
                price = acc.getPrice('steem', str_date)
                db.insert_price(date, price)
            dates[str_date] = price
        else:
            price = dates[str_date]
        data = get_curator_post_stats('cub1', post, price)
        if data != None:
            print(data)
            count += 1
            total_efficiency += data['efficiency']
            updated.append(data)
            print(f'{x}. Score: {vote["score"]}, Efficiency: {data["efficiency"]}, Avg Eff: {total_efficiency/count}, cub1_rewards: {data["cub1_rewards_steem"]}')
            if x % 10 == 0:
                db.insert_training_articles('cub1_votes', updated, False)
                print('inserted')
                updated = []

    db.insert_training_articles('cub1_votes', updated, False)

def get_account_rewards(account):
    index = -1#59551
    dates = {}
    account = Account(account)
    while not index < 0 or index == -1:
        try:
            curation_rewards = account.get_account_history(filter_by=["curation_reward"], index=index, limit=100)
        except RPCError as e:
            print(e)
            print(index)
            exit()
        entries = []
        for reward in curation_rewards:
            try:
                post = s.get_content(author=reward['comment_author'], permlink=reward['comment_permlink'])
                payout_date = datetime.datetime.strptime(post['created'], '%Y-%m-%dT%H:%M:%S') + datetime.timedelta(days=7)
                date = datetime.datetime(month=payout_date.month, day=payout_date.day, year=payout_date.year)
                str_date = date.strftime('%d-%m-%Y')
                if str_date not in dates.keys():
                    price = db.get_price(date)
                    if price == None:
                        price = acc.getPrice('steem', str_date)
                        db.insert_price(date, price)
                    dates[str_date] = price
                data = get_curator_post_stats('cub1', post, price)
                if data != None:
                    for vote in post['active_votes']:
                        if vote['voter'] == 'cub1':
                            weight = vote['percent'] // 100
                            voted_after = datetime.datetime.strptime(vote['time'], '%Y-%m-%dT%H:%M:%S') - datetime.datetime.strptime(post['created'], '%Y-%m-%dT%H:%M:%S')
                            voted_after = voted_after.seconds // 60
                            break
                    data['weight'] = weight
                    data['created'] = datetime.datetime.strptime(post['created'], '%Y-%m-%dT%H:%M:%S')
                    data['voted_after'] = voted_after
                    data['payout_date'] = payout_date
                    data['author'] = post['author']
                    data['permlink'] = post['permlink']
                    entries.append(data)
                    print(data)
            except RPCError as e:
                print(e)
        db.insert_training_articles('cub1_rewards', entries)
        index = reward['index'] - 1

def get_curator_reward_stats(curator):
    votes = db.get_data(f'{curator}_votes')
    votes['created'] = pd.to_datetime(votes['created'])
    latest_date = datetime.datetime.now() - datetime.timedelta(days=8)
    votes = votes[votes['created'] <= latest_date]
    votes = votes[votes[['efficiency', 'val_at_payout', f'{curator}_rewards_steem', f'{curator}_rewards_dollars']].isnull().any(axis=1)]
    votes = votes[votes['weight']>0]
    print(len(votes))
    dates = {}
    to_update = []
    for x in range(len(votes)-1, 0, -1):
        vote = votes.iloc[x]
        post = s.get_content(vote['author'], vote['permlink'])
        payout_date = datetime.datetime.strptime(post['created'], '%Y-%m-%dT%H:%M:%S') + datetime.timedelta(days=7)
        date = datetime.datetime(month=payout_date.month, day=payout_date.day, year=payout_date.year)
        str_date = date.strftime('%d-%m-%Y')
        if str_date not in dates.keys():
            price = db.get_price(date)
            if price == None:
                price = acc.getPrice('steem', str_date)
                db.insert_price(date, price)
            dates[str_date] = price
        else:
            price = dates[str_date]
        try:
            data = get_curator_post_stats('cub1', post, price)
        except Exception as e:
            print(e)
            data = None
        if data != None:
            to_update.append(data)
            print(x, data)
            print(f'Score: {vote["score"]} | Vote Val: {vote["val_at_vote"]} Payout Val: {data["val_at_payout"]}')
        if len(to_update) > 10:
            db.insert_training_articles(f'{curator}_votes', to_update, False)
            to_update = []
    db.insert_training_articles(f'{curator}_votes', to_update, False)

def get_actual_percentiles(curator):
    votes = db.get_data(f'{curator}_votes')
    votes = votes[votes['final_percentile'].isnull()]
    votes['created'] = pd.to_datetime(votes['created'])
    latest_date = datetime.datetime.now() - datetime.timedelta(days=8)
    votes = votes[votes['created'] <= latest_date]
    df = get_dataframe(include_zero=False, n_months=12, min_body_len=100, max_body_len=3000, upper_dollar=1, min='50_min', blacklist_authors=True, blacklist_file='../../SteemAI/blacklisted_authors.txt')
    votes_id = votes['id'].to_list()
    df_id = df['id'].to_list()
    not_in_df = [x for x in votes_id if x not in df_id]
    not_in_df_votes = votes[votes['id'].isin(not_in_df)]

    to_get_percentile = df[df['id'].isin(votes_id)][['id', 'created', 'total_value']]

    print(len(not_in_df_votes))
    for x in range(len(not_in_df_votes)):
        item = not_in_df_votes.iloc[x]
        post = s.get_content(item['author'], item['permlink'])
        new_data = pd.DataFrame({'id':[int(post['id'])],'created': [datetime.datetime.strptime(post['created'], '%Y-%m-%dT%H:%M:%S')], 'total_value':[get_post_historic_value(post, 10080)]})
        to_get_percentile = pd.concat([to_get_percentile, new_data], ignore_index=True)
        if len(to_get_percentile) > 10:
            percentiles = get_percentiles(to_get_percentile)
            print(percentiles)
    print(len(to_get_percentile))
    percentiles = get_percentiles(to_get_percentile)
    print(percentiles['percentile'])
    percentiles['final_percentile'] = percentiles['percentile']
    to_update = []
    for x in range(len(percentiles)):
        item = percentiles.iloc[x]
        print({'id':item['id'], 'final_percentile':item['percentile']})
        to_update.append({'id':item['id'], 'final_percentile':item['percentile']})
    print(len(to_update))
    db.insert_training_articles(f'{curator}_votes', to_update)

if __name__ == '__main__':
    get_actual_percentiles('cub1')

