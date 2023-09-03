from bs4 import BeautifulSoup
import enchant
from polyglot.detect import Detector
import re
import nltk
import numpy as np
from polyglot.detect.base import logger as polyglot_logger
import string
polyglot_logger.setLevel("ERROR")

def get_filtered(article):
    def filter_images_and_links(text):
        return re.sub('!?\[[-a-zA-Z0-9?@: %._\+~#=/()]*\]\([-a-zA-Z0-9?@:%._\+~#=/()]+\)', '', text)
    def filter_html_tags(text):
        CLEANR = re.compile('<.*?>')
        return re.sub(CLEANR, '', text)
    def filter_urls(text):
        return re.sub('(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]'
                      '[a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.'
                      '[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}'
                      '|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]\.[^\s]'
                      '{2,}|www\.[a-zA-Z0-9]\.[^\s]{2,})', '', text)
    def filter_special_characters(text):
        #return re.sub("[^A-Za-z0-9' @.!?ㄱㄴㄷㄹㅁㅂㅅㅇㅈㅊㅋㅌㅍㅎㅏㅑㅓㅕㅗㅛㅜㅠㅡㅣㅢㅚㅐㅟㅔㅒㅖㅘㅝㅙㅞㄲㄸㅃㅉㅆ]", '', text)
        punc = '''()-[]{};:"\,<>/@#$%^&*_~'''
        for ele in text:
            if ele in punc:
                text = text.replace(ele, "")
        return text
    def filter_formatting(text):
        text = re.sub('&?nbsp', ' ', text)
        text = re.sub('aligncenter', '', text)
        text = re.sub('styletextalign', '', text)
        return text
    raw = article['body']
    filtered = filter_images_and_links(raw)
    filtered = filter_html_tags(filtered)
    filtered = filter_urls(filtered)
    filtered = filter_formatting(filtered)
    filtered = filter_special_characters(filtered)
    return filtered


def get_body(article):
    raw_body = get_filtered(article)
    try:
        html_body = BeautifulSoup(raw_body, features='html.parser')
    except Exception as e:
        print(e)
        return []
    soup_body = html_body.get_text()
    old_body = soup_body.split()
    body = []
    for word in old_body:  # Gets rid of unwanted characters, links, and usernames
        for char in word:
            if char == "@":
                word = ""
            if char.isalpha() != True and char != "'":
                word = word.replace(char, "")
        body.append(word)
        if word == "" or "https" in word:
            body.remove(word)
    return body

def get_paragraph_count(filtered):
    def count_paragraphs(text):
        return text.count('\n\n') + 1
    return count_paragraphs(filtered)

def get_sentences(article):
    raw_body = get_filtered(article)
    try:
        html_body = BeautifulSoup(raw_body, features='html.parser')
    except Exception as e:
        print(e)
        return []
    try:
        soup_body = html_body.get_text()
    except Exception as e:
        print(e)
    sentences_raw = nltk.tokenize.sent_tokenize(soup_body)
    sentences = []
    for s in sentences_raw:
        words = s.split()
        sentence = ""
        for word in words:
            for char in word:
                if char == "@":
                    word = ""
                if char.isalpha() != True and char != "'":
                    word = word.replace(char, "")
            if not word == "" and not "https" in word:
                sentence += f" {word}"
        sentences.append(sentence)
    return sentences

def get_avg_sentence_length(sentences:list):
    avg = np.mean([len(x) for x in sentences])
    if not np.isnan(avg):
        return int(avg)
    else:
        return 0

def get_sentence_length_variance(sentences):
    var = np.var([len(x) for x in sentences])
    if not np.isnan(var):
        return int(var)
    else:
        return 0

def get_occurence_of_steem_words(body:list):
    steem_words = ['steem', 'steemit', 'splinterlands', ' curation', 'curator', 'curators', 'rshares', 'sps', 'sbd',
                   'resteem', 'upvote', 'upvotes', 'downvote', 'downvotes', 'steemd', 'smt']
    sc = 0
    for word in body:
        if word.lower() in steem_words:
            sc += 1
    return sc

def get_occurence_of_crypto_words(body:list):
    crypto_words = ['blockchain', 'steem', 'bitcoin', 'ethereum', 'litecoin', 'hive', 'mined', 'stake', 'hardfork',
                    'token', 'tokens',
                    'crypto', 'btc', 'eth', 'ether', 'ltc', 'dogecoin', 'coinbase', 'nft', 'encryption', 'encrypted',
                    'tron',
                    'trx', 'stake', 'staked']
    cc = 0
    for word in body:
        if word.lower() in crypto_words:
            cc += 1
    return cc

def get_occurence_of_hive_words(body:list):
    hive_words = ['hive', 'peakd', 'hbd', 'reblog', 'hive-engine', 'dao']
    hc = 0
    for word in body:
        if word.lower() in hive_words:
            hc+=1
    return hc

async def get_language_and_spelling_errors(sentences) -> tuple:
    languages = {}
    word_list = {}
    errors = 0
    x = 0
    while x < len(sentences):
        sentence = sentences[x]
        while len(sentence.replace(" ", "")) < 5 and x < len(sentences):
            x += 1
            if x < len(sentences) - 1:
                sentence += sentences[x]
        if len(sentence) >= 5:
            lang = Detector(sentence).language.code
            words = sentence.split()
            if lang in word_list:
                word_list[lang] += words
            else:
                word_list[lang] = []
                word_list[lang] += words
        x+=1
    for lang in word_list:
        words = word_list[lang]
        try:
            d = enchant.Dict(lang)
            for word in words:
                if d.check(word.lower()) == True:
                    if lang in languages:
                        languages[lang] += 1
                    else:
                        languages[lang] = 1
                else:
                    errors += 1
        except enchant.errors.DictNotFoundError:
            errors = errors
    if len(languages) > 0:
        primary = max(languages, key=languages.get)
    else:
        primary = "other"
    return primary, errors



def get_spelling_error_percent(spelling_errors: int, word_count: int) -> int:
    if word_count > 0:
        return int(spelling_errors/word_count * 100)
    else:
        return 0