from steem import Steem
from datetime import datetime, timedelta
from Downloading.SteemPostData import check_post_func

s = Steem()

def get_articles_in_time_frame(min_age:int, max_age:int, last_author:str=None, last_permlink:int=None, blacklisted_authors=[]):
    current_time = datetime.utcnow()
    min_time = current_time - timedelta(minutes=min_age)
    max_time = current_time - timedelta(minutes=max_age)
    query = {
        "limit": 100,  # number of posts
        "tag": ""  # tag of posts
    }
    found = False
    final_articles = []
    while not found:
        articles = s.get_discussions_by_created(query)
        last_article = articles[-1]
        last_article['Created'] = datetime.strptime(last_article['created'], "%Y-%m-%dT%H:%M:%S")
        if last_article['Created'] > max_time and last_article['Created'] <= min_time:
            query['start_permlink'] = last_article['permlink']
            query['start_author'] = last_article['author']
            articles.pop(-1)
        if last_article['Created'] > min_time:
            query['start_permlink'] = last_article['permlink']
            query['start_author'] = last_article['author']
            articles.pop(-1)
        else:
            for article in articles:
                if 'Created' not in article.keys():
                    article['Created'] = datetime.strptime(article['created'], "%Y-%m-%dT%H:%M:%S")
                if article['Created'] <= min_time and article['Created'] >= max_time:
                    p = s.get_content(article['author'], article['permlink'])
                    if check_post_func(p, blacklisted_authors=blacklisted_authors):
                        final_articles.append(p)
                if article['Created'] < max_time:
                    if last_permlink != None and last_author != None:
                        for x in range(len(final_articles)):
                            post = final_articles[x]
                            if post['author']==last_author and post['permlink'] == last_permlink:
                                final_articles = final_articles[x+1:]
                                return final_articles
                    return final_articles