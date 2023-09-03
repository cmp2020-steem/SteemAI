import threading
import pandas as pd
from Downloading.Blockchain import get_block
from steem import Steem
import queue
from steembase.exceptions import RPCError
from steem import blockchain
from time import time, sleep
from datetime import datetime, timedelta
from Downloading.SteemSQL import SSQL
from Downloading.SteemPostData import get_post_data
import json
from Downloading.SteemPostData import check_post
from Downloading.multiProcessSteemWorld import poolProcessNetwork
import asyncio
from multiprocessing import Pool
import aiohttp
from aiolimiter import AsyncLimiter
import os
import fcntl
import requests.exceptions
s = Steem()
b = blockchain.Blockchain()
db = SSQL()

def get_unprocessed_posts():
    unprocessed_posts = []
    path = 'updatingData'
    while True:
        # Open the file in read-write mode
        with open('data.json', 'r+') as file:
            try:
                # Try to acquire an exclusive lock on the file
                fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)

                with open('data.json', 'r') as f:
                    lines = f.readlines()
                for line in lines:
                    unprocessed_posts.append(json.loads(line))
                with open('data.json', 'w') as f:
                    f.writelines([])
                with open('dataLen.txt', 'w') as f:
                    f.writelines(['0'])

                # Perform the file modifications here

                # Release the lock when finished
                fcntl.flock(file, fcntl.LOCK_UN)

                # Exit the loop so the program can exit
                return unprocessed_posts
            except IOError:
                # If the lock can't be acquired, sleep for a short time and try again
                sleep(0.1)

def processPosts():
    total_processing_time = 0
    total_processed_posts = 0
    total_insertion_time = 0
    total_inserted_posts = 0
    total_insertions = 0
    posts_processed = 0
    processed_posts = []
    total_batch_time = 0
    total_batch_size = 0
    sleep(15)
    while True:
        with open('dataLen.txt', 'r') as f:
            lines = f.readlines()
        if len(lines) > 0:
            file_len = int(lines[0])
        else:
            file_len = 0
        if file_len >= 5:
            unprocessed_posts = get_unprocessed_posts()
            batch_start = time()
            posts_to_process = []
            for x in range(len(unprocessed_posts)):
                posts_to_process.append(unprocessed_posts.pop(0))
            start = time()
            with Pool(5) as p:
                returned_posts = p.map(poolProcessPosts, posts_to_process)
                for post in returned_posts:
                    processed_posts.append(post)
                    if 'total_value' in list(post.keys()):
                        print(f"{len(processed_posts)}. Title: {post['title']} | Date: {post['created']} | Language: {post['primary_language']} | Total Value: {post['total_value']}")
                    else:
                        print(f"{len(processed_posts)}. Date: {post['created']} | Language: {post['primary_language']}")
            end = time()
            posts_processed += len(posts_to_process)
            processed = end-start
            total_processing_time += processed
            total_processed_posts += len(posts_to_process)
            if total_processed_posts > 0:
                if len(posts_to_process) > 0:
                    print(f"{len(posts_to_process)} posts processed | Processed in: {processed}s | Average Processing Time: {total_processing_time/total_processed_posts}s")
            if len(processed_posts) >= 5:
                batch_end = time()
                total_insertion_time += insertPosts('TrainingBlockchainData', processed_posts)
                total_inserted_posts += len(processed_posts)
                total_insertions += 1
                total_batch_time += batch_end - batch_start
                total_batch_size += 1
                print(f'Average Insertion time {total_insertion_time/total_insertions} | Batch processed in {batch_end - batch_start}s | Batch Avg: {total_batch_time/total_batch_size}s | Avg Batch Size: {total_inserted_posts // total_batch_size}')
                batch_start = time()
                processed_posts = []
                posts_processed = 0
        else:
            sleep(15)


def poolProcessPosts(item):
    post, block = item, item['block']
    return get_post_data(post, block)

def insertPosts(table, posts):
    total_inserting_time = 0
    total_inserted_posts = 0
    print('insert')
    start = time()
    rec = db.insert_training_articles(table, posts)
    end = time()
    inserted = end-start
    total_inserting_time += inserted
    total_inserted_posts += 1
    print(f"{len(posts)} posts inserted in: {inserted}s")
    return inserted

processPosts()