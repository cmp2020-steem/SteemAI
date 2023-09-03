from steem import Steem
from steem.account import Account
from datetime import datetime, timedelta
from steembase.exceptions import RPCError
from Downloading import SteemPostData as spd
from Downloading.SteemSQL import SSQL
import requests
import re
#from SteemSQL import SSQL
from pycoingecko import CoinGeckoAPI
from steem.converter import Converter
from Downloading import timeFunctions as tf
from Downloading.timeFunctions import convertTime, convertTimeStamp, getTime
import asyncio
import time
import aiohttp
import asyncio
from steem import Steem
from difflib import SequenceMatcher
from aiolimiter import AsyncLimiter
import pandas as pd

s = Steem()
'''import logging

import http.client
http.client.HTTPConnection.debuglevel = 1

# You must initialize logging, otherwise you'll not see debug output.
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True'''




converter = Converter()
DB = SSQL()

cg = CoinGeckoAPI()

def check_similar(a, b):
    return SequenceMatcher(None, a, b).ratio()


async def download_link(url:str,session):
    tries = 0
    while tries <= 100:
        try:
            async with session.get(url, timeout=2) as response:
                result = await response.json()
                return result
        except asyncio.exceptions.TimeoutError:
            tries+=1

def get_block(date):
    now = datetime.now()
    difference = now - date
    current = requests.get('https://sds.steemworld.org/blocks_api/getLastIrreversibleBlockNum').json()['result']
    return int(current - (difference.days * 28800) - (difference.seconds * 1/3))

def get_block_range(start, end):
    found = False
    first = start
    if end > start + 249:
        last = first + 249
    else:
        last = end
    while not found and first < last and last <= end:
        response = requests.get(f'https://sds.steemworld.org/blocks_api/getVirtualOpsInBlockRange/{first}-{last}')
        rows = response.json()['result'][0]
        for block in rows:
            if block[0] == 'fill_vesting_withdraw':
                info = block[1]
                if 'STEEM' in info['deposited']:
                    vests = float(re.findall("\d+.\d+", f"{info['withdrawn']}")[0])
                    steem = float(re.findall("\d+.\d+", f"{info['deposited']}")[0])

                    if steem > 2:
                        return vests/steem

        first = last + 1
        if first + 249 < end:
            last = first + 249
        else:
            last = end

async def get_post_historic_resteems(post, session):
    created = tf.convertTime(post['created'])
    timestampe_60_min = tf.getTime(created + timedelta(minutes=60))
    response = await download_link(f'https://sds.steemworld.org/post_resteems_api/getResteems/{post["author"]}/{post["permlink"]}', session)
    if 'code' in response.keys():
        if response['code'] == -1:
            return 0
    rows = response['result']['rows']

    if rows == None:
        return 0
    resteems_60_min = 0
    for row in rows:
        if row[0] <= timestampe_60_min:
            resteems_60_min+=1
    return resteems_60_min

def getPrice(coin, date):
    history = cg.get_coin_history_by_id(id=coin, date=date)
    price = history["market_data"]["current_price"]["usd"]
    return price

def get_closest_index(acct, article_block: int):
    account = Account(acct)
    while True:
        try:
            history = account.get_account_history(-1, 0)
            break
        except RPCError as e:
            print(e)
            time.sleep(1)
    for h in history:
        upper = h['index']
    lower = 0
    while lower < upper - 1:
        index = (upper + lower) // 2
        while True:
            try:
                history = account.get_account_history(index, 0)
                break
            except RPCError as e:
                print(e)
                time.sleep(1)
        for h in history:
            block = h['block']
        if block > article_block:
            upper = index
            lower = lower
        elif block < article_block:
            upper = upper
            lower = index
        else:
            return index
    return index

async def get_author_followers(post):
    current_followers = len(requests.get(f'https://sds.steemworld.org/followers_api/getFollowers/{post["author"]}').json()['result'])
    timestamp = getTime(convertTime(post['created']))
    added_followers = len(requests.get(f'https://sds.steemworld.org/followers_api/getFollowedHistory/{post["author"]}/{timestamp}-{int(time())}').json()['result']['rows'])
    return current_followers - added_followers

async def get_account_followed_by(post, accounts, session):
    followed_by = {}
    timestamp = getTime(convertTime(post['created']))
    check = await download_link(f'https://sds.steemworld.org/followers_api/getFollowers/{post["author"]}', session)
    check = check['result']
    added = await download_link(f'https://sds.steemworld.org/followers_api/getFollowedHistory/{post["author"]}/{timestamp}-{int(time.time())}', session)
    try:
        added = added['result']['rows']
        for account in accounts:
            if account in check:
                if account in added:
                    followed_by[account] = False
                else:
                    followed_by[account] = True
            else:
                followed_by[account] = False
        return len(check) - len(added), followed_by
    except KeyError:
        for account in accounts:
            followed_by[account] = False
        return 0, followed_by

async def get_account_gained_followers(post, n_days=7, session=None):
    created = getTime(convertTime(post['created']))
    n_before = getTime(convertTime(post['created']) - timedelta(n_days))
    week_before = getTime(convertTime(post['created']) - timedelta(7))
    res = await download_link(f'https://sds.steemworld.org/followers_api/getFollowedHistory/{post["author"]}/{n_before}-{created}', session)
    gained_followers = len(res['result']['rows'])
    gained_followers_1_week = 0
    for item in res['result']['rows']:
        if item[0] >= week_before:
            gained_followers_1_week += 1
    return gained_followers, gained_followers_1_week

async def get_account_age(post, session):
    post_created = convertTime(post['created'])
    req = await download_link(f'https://sds.steemworld.org/accounts_api/getAccount/{post["author"]}', session)
    account_created = convertTime(convertTimeStamp(req['result']['created']))
    account_age = post_created - account_created
    return account_age.days

async def get_author_rewards(post:str, n:int=30, session=None):
    timestamp = convertTime(post['created'])
    start = getTime(timestamp - timedelta(n))
    end = getTime(timestamp)
    timestamp_1_week = getTime(timestamp - timedelta(7))
    response = await download_link(
        f'https://sds.steemworld.org/rewards_api/getRewards/author_reward/{post["author"]}/{start}-{end}', session)
    rows = response['result']['rows']
    month_rewards = []
    week_rewards = []
    similar_month_rewards = []
    similar_week_rewards = []
    total_1_month = 0
    total_1_week = 0
    min_1_month = None
    max_1_month = 0
    similar_total_1_month = 0
    similar_total_1_week = 0
    similar_min_1_month = None
    similar_max_1_month = 0
    count_1_month = 0
    count_1_week = 0
    similar_count_1_month = 0
    similar_count_1_week = 0
    min_1_week = None
    max_1_week = 0
    similar_min_1_week = None
    similar_max_1_week = 0
    permlink = post['permlink']
    for reward in rows:
        rew_permlink = reward[2]
        rew = reward[5]
        first_half_permlink = permlink[:len(permlink)//2]
        first_half_rew_permlink = rew_permlink[:len(rew_permlink)//2]
        similarity_first_half = check_similar(first_half_permlink, first_half_rew_permlink)
        similarity = check_similar(permlink, rew_permlink)
        if similarity >= 0.6 or similarity_first_half > 0.8:
            similar_month_rewards.append(rew)
            similar_total_1_month += rew
            similar_count_1_month += 1
            if similar_min_1_month == None:
                similar_min_1_month = rew
            if rew > similar_max_1_month:
                similar_max_1_month = rew
            elif rew < similar_min_1_month:
                similar_min_1_month = rew
            if reward[0] >= timestamp_1_week:
                similar_week_rewards.append(rew)
                similar_total_1_week += rew
                similar_count_1_week += 1
                if similar_min_1_week == None:
                    similar_min_1_week = rew
                if rew > similar_max_1_week:
                    similar_max_1_week = rew
                elif rew < similar_min_1_week:
                    similar_min_1_week = rew
        similar_month_rewards.append(rew)
        total_1_month += rew #index 5 is vests
        if min_1_month == None:
            min_1_month = rew
        if rew < min_1_month:
            min_1_month = rew
        elif rew > max_1_month:
            max_1_month = rew
        count_1_month += 1
        if reward[0] >= timestamp_1_week:
            similar_week_rewards.append(rew)
            if min_1_week == None:
                min_1_week = rew
            if rew < min_1_week:
                min_1_week = rew
            elif rew > max_1_week:
                max_1_week = rew
            total_1_week += rew
            count_1_week += 1
    avg_1_month = 0
    avg_1_week = 0
    similar_avg_1_month = 0
    similar_avg_1_week = 0
    if count_1_month != 0:
        avg_1_month = total_1_month // count_1_month
    if count_1_week != 0:
        avg_1_week = total_1_week // count_1_week
    if similar_count_1_month != 0:
        similar_avg_1_month = similar_total_1_month // similar_count_1_month
    if similar_count_1_week != 0:
        similar_avg_1_week = similar_total_1_week // similar_count_1_week

    if min_1_week == None:
        min_1_week = 0
    if min_1_month == None:
        min_1_month = 0
    if similar_min_1_week == None:
        similar_min_1_week = 0
    if similar_min_1_month == None:
        similar_min_1_month = 0

    similar_month_median = 0
    similar_week_median = 0
    month_median = 0
    week_median = 0

    if len(similar_month_rewards) > 0:
        similar_month_rewards.sort()
        median_index = len(similar_month_rewards) // 2
        similar_month_median = similar_month_rewards[median_index]
    if len(similar_week_rewards) > 0:
        similar_week_rewards.sort()
        median_index = len(similar_week_rewards) // 2
        similar_week_median = similar_week_rewards[median_index]
    if len(month_rewards) > 0:
        month_rewards.sort()
        median_index = len(month_rewards) // 2
        month_median = month_rewards[median_index]
    if len(week_rewards) > 0:
        week_rewards.sort()
        median_index = len(week_rewards) // 2
        week_median = week_rewards[median_index]

    history = {}
    history['avg_author_rewards_1_month'], history['median_author_rewards_1_month'], history['author_total_posts_1_month'] = avg_1_month, month_median, count_1_month
    history['max_author_rewards_1_month'], history['min_author_rewards_1_month'] = max_1_month, min_1_month
    history['avg_author_rewards_1_week'], history['median_author_rewards_1_week'], history['author_total_posts_1_week'] = avg_1_week, week_median, count_1_week
    history['max_author_rewards_1_week'], history['min_author_rewards_1_week'] = max_1_week, min_1_week
    history['similar_avg_author_rewards_1_month'], history['similar_median_author_rewards_1_month'], history['similar_author_total_posts_1_month'] = similar_avg_1_month, similar_month_median,similar_count_1_month
    history['similar_max_author_rewards_1_month'], history['similar_min_author_rewards_1_month'] = similar_max_1_month, similar_min_1_month
    history['similar_avg_author_rewards_1_week'], history['similar_median_author_rewards_1_week'], history['similar_author_total_posts_1_week'] = similar_avg_1_week, similar_week_median, similar_count_1_week
    history['similar_max_author_rewards_1_week'], history['similar_min_author_rewards_1_week'] = similar_max_1_week, similar_min_1_week

    return history

async def get_old_author_rewards(post:str, n:int = 30, session=None):
    timestamp = convertTime(post['created'])
    start = getTime(timestamp - timedelta(n))
    end = getTime(timestamp)
    timestamp_1_week = getTime(timestamp - timedelta(7))
    response = await download_link(f'https://sds.steemworld.org/rewards_api/getRewards/author_reward/{post["author"]}/{start}-{end}', session)
    rows = response['result']['rows']
    total_1_month = 0
    total_1_week = 0
    min_1_month = 0
    max_1_month = 0
    count_1_month = 0
    count_1_week = 0
    min_1_week = 0
    max_1_week = 0
    for reward in rows:
        total_1_month += reward[5] #index 5 is vests
        if reward[5] < min_1_month:
            min_1_month = reward[5]
        elif reward[5] > max_1_month:
            max_1_month = reward[5]
        count_1_month += 1
        if reward[0] >= timestamp_1_week:
            if reward[5] < min_1_week:
                min_1_week = reward[5]
            elif reward[5] > max_1_week:
                max_1_week = reward[5]
            total_1_week += reward[5]
            count_1_week += 1
    if count_1_month != 0 and count_1_week !=0:
        return total_1_month // count_1_month, total_1_week // count_1_week, count_1_month, count_1_week, int(round(max_1_month)), int(round(max_1_week)), int(round(min_1_month)), int(round(min_1_week))
    elif count_1_month != 0 and count_1_week == 0:
        return total_1_month // count_1_month, 0, count_1_month, 0, int(round(max_1_month)), int(round(max_1_week)), int(round(min_1_week)), int(round(min_1_week))
    else:
        return 0, 0, 0, 0, 0, 0, 0, 0

async def get_curator_rewards_dollars(curator:str, time:str, n:int=30, session=None):
    if curator == None:
        return 0.0
    timestamp = convertTime(time)
    start = getTime(timestamp - timedelta(n))
    end = getTime(timestamp)
    response = await download_link(f'https://sds.steemworld.org/rewards_api/getRewards/author_reward/{curator}/{start}-{end}', session)
    rows = response['result']['rows']
    total_vests = 0
    total_rewards = 0
    for r in rows:
        total_vests += r[1]
        total_rewards += 1
    if total_rewards != 0:
        return total_vests // total_rewards
    else:
        return 0.0

async def get_curator_rewards_vests(curator:str, time:str, n:int=30, session=None):
    if curator == None or curator == 'None':
        return {'avg_rewards_1_week':0, 'median_rewards_1_week':0, 'min_rewards_1_week':0, 'max_rewards_1_week':0}
    timestamp = convertTime(time)
    start = getTime(timestamp - timedelta(n))
    end = getTime(timestamp)
    response = await download_link(f'https://sds.steemworld.org/rewards_api/getRewards/curation_reward/{curator}/{start}-{end}', session)
    try:
        rows = response['result']['rows']
    except Exception:
        print(response)
        rows = []
    rewards = []
    total_vests = 0
    total_rewards = 0
    for r in rows:
        current_vests = r[1]
        rewards.append(current_vests)
        total_vests += current_vests
        total_rewards += 1
    if len(rewards) > 0:
        rewards.sort()
        median_index = len(rewards) // 2
        avg_rewards = total_vests // total_rewards
        median_rewards = rewards[median_index]
        min_rewards = rewards[0]
        max_rewards = rewards[-1]
    else:
        avg_rewards, total_rewards, median_rewards, min_rewards, max_rewards = 0, 0, 0, 0, 0

    return {'avg_rewards_1_week':avg_rewards, 'total_rewards_1_week':total_rewards, 'median_rewards_1_week':median_rewards,
            'min_rewards_1_week':min_rewards, 'max_rewards_1_week':max_rewards}

async def test():
    my_conn = aiohttp.TCPConnector(limit=10)
    limiter = AsyncLimiter(8, 1)
    async with aiohttp.ClientSession(connector=my_conn) as session:
        async with limiter:
            post = s.get_content('yourloveguru', 'contest-2-or-share-your-fashion-item-which-is-outdated-but-you-still-keep-it')
            res = await get_author_rewards(post, 30, session)
            print(res)

