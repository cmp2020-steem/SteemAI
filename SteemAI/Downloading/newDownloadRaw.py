import threading
import pandas as pd
from Downloading.Blockchain import get_block
from steem import Steem
import queue
from steembase.exceptions import RPCError
from steem import blockchain
from time import time, sleep
from datetime import datetime, timedelta
import json
import os
from Downloading.SteemSQL import SSQL
from Downloading.SteemPostData import get_post_data
from Downloading.SteemPostData import check_post
import fcntl
from Downloading.multiProcessSteemWorld import poolProcessNetwork
import asyncio
from multiprocessing import Pool
import aiohttp
from aiolimiter import AsyncLimiter

db = SSQL()
s = Steem()

def downloadPosts(start:int=36000000, end:int=None):
    chain = blockchain.Blockchain()

    while start > -1:
        for block in chain.history(filter_by="comment", start_block=start, end_block=end):
            block_time = block['timestamp']
            most = datetime.utcnow() - timedelta(days=7)
            if block_time >= most:
                time_to_wait = (block_time - most).total_seconds() + 5
                print(f'Waiting {time_to_wait} seconds')
                sleep(time_to_wait)
            start += 1
            if block["parent_author"] == "":
                start = block['block_num']
                author = block["author"]
                permlink = block["permlink"]
                while True:
                    try:
                        post = s.get_content(author, permlink)
                        break
                    except Exception as e:
                        print('inner exception')
                        print(e)
                        sleep(5)
                        print('inner done sleeping')
                check = check_post(post)
                if check:
                    while True:
                        # Open the file in read-write mode
                        with open('data.json', 'r+') as file:
                            try:
                                # Try to acquire an exclusive lock on the file
                                fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)

                                post['block'] = block['block_num']
                                with open('data.json', 'a') as f:
                                    json.dump(post, f)
                                    f.write('\n')
                                with open('dataLen.txt', 'r') as f:
                                    lines = f.readlines()
                                try:
                                    file_len = int(lines[0])
                                except Exception:
                                    file_len = 0
                                file_len += 1
                                lines[0] = str(file_len)
                                with open('dataLen.txt', 'w') as f:
                                    f.writelines(lines)
                                print(post['created'])

                                # Perform the file modifications here

                                # Release the lock when finished
                                fcntl.flock(file, fcntl.LOCK_UN)

                                # Exit the loop so the program can exit
                                break
                            except IOError:
                                # If the lock can't be acquired, sleep for a short time and try again
                                sleep(0.1)
        '''except Exception as e:
            start += 1
            print('outer exception')
            print(e)
            sleep(5)
            print('outer done sleeping')'''

df = db.get_data('TrainingBlockchainData')
props = s.get_dynamic_global_properties()
start = get_block(9, props['head_block_number'])
downloadPosts(start)