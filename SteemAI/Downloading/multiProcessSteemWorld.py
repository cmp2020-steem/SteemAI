from Downloading import AccountInfo as acc
import pandas as pd
from Downloading import SteemSQL
from steem import Steem
import time
import asyncio
import aiohttp
from aiolimiter import AsyncLimiter

DB = SteemSQL.SSQL()
s = Steem()

ACCOUNTS = ['steemcurator01', 'steemchiller', 'rme', 'chriddi']

async def get_historic_data_steem_world(post):
    my_conn = aiohttp.TCPConnector(limit=10)
    limiter = AsyncLimiter(8, 1)
    async with aiohttp.ClientSession(connector=my_conn) as session:
        async with limiter:
            data = {}
            author_rewards_history = await acc.get_author_rewards(post, 30, session=session)
            for key in author_rewards_history.keys():
                data[key] = author_rewards_history[key]
            data['author_account_age'] = await acc.get_account_age(post, session)
            data['author_followers'], followed_by = await acc.get_account_followed_by(post, ACCOUNTS, session)
            for account in ACCOUNTS:
                data[f'followed_by_{account}'] = int(followed_by[account])
            data['gained_followers_1_month'], data['gained_followers_1_week'] = await acc.get_account_gained_followers(
                post, 30, session)
    return data

async def get_time_data_steem_world(post, time=60):
    data = {}
    my_conn = aiohttp.TCPConnector(limit=10)
    limiter = AsyncLimiter(8, 1)
    async with aiohttp.ClientSession(connector=my_conn) as session:
        async with limiter:
            median_voter_50_min_rewards_history = await acc.get_curator_rewards_vests(post[f'50_min_median_voter'],
                                                                                      post['created'], 7, session)
            for key in median_voter_50_min_rewards_history.keys():
                data[f'50_min_median_voter_{key}'] = median_voter_50_min_rewards_history[key]

            top_voter_60_min_rewards_history = await acc.get_curator_rewards_vests(post['50_min_top_voter'],
                                                                                   post['created'], 7,
                                                                                   session)
            for key in top_voter_60_min_rewards_history.keys():
                data[f'50_min_top_voter_{key}'] = top_voter_60_min_rewards_history[key]
            if time == 60:
                median_voter_60_min_rewards_history = await acc.get_curator_rewards_vests(post[f'60_min_median_voter'],
                                                                                          post['created'], 7, session)
                for key in median_voter_60_min_rewards_history.keys():
                    data[f'60_min_median_voter_{key}'] = median_voter_60_min_rewards_history[key]

                top_voter_60_min_rewards_history = await acc.get_curator_rewards_vests(post['60_min_top_voter'],
                                                                                       post['created'], 7,
                                                                                       session)
                for key in top_voter_60_min_rewards_history.keys():
                    data[f'60_min_top_voter_{key}'] = top_voter_60_min_rewards_history[key]
                data['60_min_resteems'] = await acc.get_post_historic_resteems(post, session)
    return data


async def poolProcessNetwork(post, session):
    data = {'id':post['id']}
    author_rewards_history = await acc.get_author_rewards(post, 30, session)
    for key in author_rewards_history.keys():
        data[key] = author_rewards_history[key]

    data['author_account_age'] = await acc.get_account_age(post, session)

    median_voter_4_min_30_sec_rewards_history = await acc.get_curator_rewards_vests(post['4_min_30_sec_median_voter'], post['created'], 7, session)
    for key in median_voter_4_min_30_sec_rewards_history.keys():
        data[f'4_min_30_sec_median_voter_{key}'] = median_voter_4_min_30_sec_rewards_history[key]

    top_voter_4_min_30_sec_rewards_history = await acc.get_curator_rewards_vests(post['4_min_30_sec_top_voter'], post['created'], 7, session)
    for key in top_voter_4_min_30_sec_rewards_history.keys():
        data[f'4_min_30_sec_top_voter_{key}'] = top_voter_4_min_30_sec_rewards_history[key]

    median_voter_50_min_rewards_history = await acc.get_curator_rewards_vests(post['50_min_median_voter'], post['created'], 7, session)
    for key in median_voter_50_min_rewards_history.keys():
        data[f'50_min_median_voter_{key}'] = median_voter_50_min_rewards_history[key]

    top_voter_50_min_rewards_history = await acc.get_curator_rewards_vests(post['50_min_top_voter'], post['created'], 7, session)
    for key in top_voter_50_min_rewards_history.keys():
        data[f'50_min_top_voter_{key}'] = top_voter_50_min_rewards_history[key]

    median_voter_60_min_rewards_history = await acc.get_curator_rewards_vests(post['60_min_median_voter'],
                                                                              post['created'], 7, session)
    for key in median_voter_60_min_rewards_history.keys():
        data[f'60_min_median_voter_{key}'] = median_voter_60_min_rewards_history[key]

    top_voter_60_min_rewards_history = await acc.get_curator_rewards_vests(post['60_min_top_voter'], post['created'], 7,
                                                                           session)
    for key in top_voter_60_min_rewards_history.keys():
        data[f'60_min_top_voter_{key}'] = top_voter_60_min_rewards_history[key]

    data['60_min_resteems'] = await acc.get_post_historic_resteems(post, session)
    data['author_followers'], followed_by = await acc.get_account_followed_by(post, ACCOUNTS, session)
    for account in ACCOUNTS:
        data[f'followed_by_{account}'] = int(followed_by[account])
    data['gained_followers_1_month'], data['gained_followers_1_week'] = await acc.get_account_gained_followers(post, 30, session)
    return data



def insertPosts(table, posts):
    total_inserting_time = 0
    total_inserted_posts = 0
    start = time.time()
    df = DB.insert_training_articles(table, posts)
    end = time.time()
    inserted = end-start
    total_inserting_time += inserted
    print(f"Length of DF: {len(df)} | {len(posts)} posts inserted in: {inserted}s")
    return inserted

async def getSteemWorldData():
    my_conn = aiohttp.TCPConnector(limit=10)
    limiter = AsyncLimiter(8, 1)
    inserts = []
    exceptions = []
    async with aiohttp.ClientSession(connector=my_conn) as session:
        async with limiter:
            total_time = 0
            total_posts = 0
            while True:
                df = DB.get_data_not_matching()
                df['created'] = pd.to_datetime(df['created'])

                if len(df) > 0:
                    for x in range(len(df)):
                        if len(inserts) % 10 == 0 and len(inserts) != 0:
                            print(len(inserts))
                            if total_posts != 0:
                                print(x)
                                print(f'Average Processing: {total_time/total_posts}s')
                        post = df.iloc[x]
                        start = time.time()
                        try:
                            data = await poolProcessNetwork(post, session)
                            inserts.append(data)
                            end = time.time()
                            elapsed = end - start
                            total_time += elapsed
                            total_posts += 1
                        except Exception as e:
                            print(e)

                        '''except Exception as e:
                            print(e)
                            with open('exceptions.txt', "a+") as f:
                                f.write(f'\n{x}')'''
                        if len(inserts) >= 5:
                            print(f'Progress: {(x / len(df)) * 100}%')
                            insertPosts('SteemWorldData', inserts)
                            inserts = []
                        try:
                            print(f'{x}. Processed in {elapsed} | Average Processing {total_time / total_posts}')
                        except Exception as e:
                            print('exception')
                            print(e)
                else:
                    time.sleep(30)
                if len(inserts) > 0:
                    insertPosts('SteemWorldData', inserts)
                print(exceptions)

'2022-05-26T06:58:24'
if __name__ == '__main__':
    asyncio.run(getSteemWorldData())

'https://sds.steemworld.org/followers_api/getFollowedHistory/cmp2020/1662300289-1662905089'
'https://sds.steemworld.org/followers_api/getFollowedHistory/emsonic/1644178506-1646770506'