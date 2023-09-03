from steem import Steem
from Downloading.SteemPostData import get_historic_data, get_60_min_data, stream_check_post
from datetime import datetime, timedelta
import os
import threading
from Downloading.SteemSQL import SSQL
import time
import csv

def read_live_file_ids():
    with open('../live_posts.csv', 'r') as csvfile:
        # Create a reader object
        reader = csv.reader(csvfile)

        # Iterate through the rows of the CSV file
        live_ids = [int(x) for row in reader for x in row]
    return live_ids

def write_live_file_ids(ids):
    update_file = True
    path = '../file_in_use.txt'
    attempts = 0
    while update_file:
        attempts += 1
        if not os.path.exists(path):
            with open(path, 'w+') as f:
                pass
            with open('../live_posts.csv', 'w+') as f:
                writer = csv.writer(f)
                for row in ids:
                    if type(row) != list:
                        row = [row]
                    writer.writerow(row)
                os.remove(path)
                update_file = False
        else:
            time.sleep(1)
            if attempts > 10:
                print(f'{attempts} attempts to update!')
            update_file = True

def update_live_file_id(id):
    id_list = read_live_file_ids()
    id_list.append(id)
    write_live_file_ids(id_list)

def remove_live_file_id(id):
    live_ids = read_live_file_ids()
    if id in live_ids:
        live_ids.remove(id)
        write_live_file_ids(live_ids)

s = Steem()

db = SSQL()

streamed_posts = []
queued_posts = []

def stream_posts(event):
    event.wait(15)
    while True:
        try:
            for c in s.stream_comments():
                perm_str = c.__str__()
                split = perm_str.replace('Post-', '').replace('>', '').replace('<', '').split('/')
                author, permlink = split[0], split[1]
                raw = s.get_content(author, permlink)
                if stream_check_post(raw):
                    print(raw['created'])
                    post = get_historic_data(raw)
                    streamed_posts.append(post)
                    print(f'Streamed: {len(streamed_posts)}')
        except Exception as e:
            event.wait(15)
            print(e)

def process_streamed_posts(event):
    while True:
        for item in streamed_posts:
            print('Evaluating streamed post:', streamed_posts.index(item))
            if item['created'] <= datetime.utcnow() - timedelta(minutes=60) and item['created'] >= datetime.utcnow() - timedelta(minutes=70):
                print('Post age good')
                try:
                    original_post = get_60_min_data(item)
                    print(original_post)
                    if original_post != None:
                        queued_posts.append(original_post)
                        if item in streamed_posts:
                            streamed_posts.remove(item)
                        db.insert_training_articles('LivePosts', [original_post], False)
                        print('inserted')
                        update_live_file_id(original_post['id'])
                    else:
                        if original_post in queued_posts:
                            queued_posts.remove(original_post)
                        if item in streamed_posts:
                            streamed_posts.remove(item)
                except Exception as e:
                    print(e)
                    break
            elif item['created'] > datetime.utcnow() - timedelta(minutes=60):
                age = datetime.utcnow() - item['created']
                time_to_wait = timedelta(minutes=60) - age
                event.wait(time_to_wait.seconds)
                break
            else:
                print('removing post age:', age)
                streamed_posts.remove(item)

class StreamThread(threading.Thread):
    def __init__(self, threadId, name):
        threading.Thread.__init__(self)
        self.threadId = threadId
        self.name = name

    def run(self):
        event = threading.Event()
        print(f'starting {self.name}')
        stream_posts(event)

class ProcessThread(threading.Thread):
    def __init__(self, threadId, name):
        threading.Thread.__init__(self)
        self.threadId = threadId
        self.name = name

    def run(self):
        event = threading.Event()
        print(f'starting {self.name}')
        process_streamed_posts(event)

def main():
    stream = StreamThread(1, 'Stream')
    process = ProcessThread(2, 'Process')
    stream.start()
    process.start()
    stream.join()
    process.join()

if __name__ == '__main__':
    main()