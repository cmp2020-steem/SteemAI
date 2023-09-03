import mysql.connector
import pandas as pd
import pytz
import datetime
import traceback
import asyncio
from mysql.connector.errors import ProgrammingError

def convert(dte, fromZone, toZone):
    fromZone, toZone = pytz.timezone(fromZone), pytz.timezone(toZone)
    return fromZone.localize(dte, is_dst=True).astimezone(toZone)

#sudo service mysql start in terminal

'''db = mysql.connector.connect(
    host="localhost", #change if using server
    user="cmp2020",
    passwd="Schubert1-31",
    database="SteemSQL"
)

mycursor = db.cursor()'''


'''
mycursor.execute("""CREATE TABLE TrainingData (title VARCHAR(255), link TEXT, title_word_count smallint UNSIGNED, body_word_count smallint UNSIGNED, avg_sentence_length smallint UNSIGNED, 
body_paragraph_count smallint UNSIGNED, avg_paragraph_length smallint UNSIGNED, img_count smallint UNSIGNED, words_per_img smallint UNSIGNED,
sentence_length_variance smallint UNSIGNED, steem_word_count smallint UNSIGNED, crypto_word_count smallint UNSIGNED, hive_word_count smallint UNSIGNED,
percent_steem_words smallint UNSIGNED, steem_words_per_paragraph smallint UNSIGNED, primary_language VARCHAR(15), spelling_errors smallint UNSIGNED, 
spelling_percent smallint UNSIGNED, spelling_errors_per_paragraph smallint UNSIGNED, spelling_errors_per_sentence smallint UNSIGNED ,created VARCHAR(25), day smallint UNSIGNED,
hour smallint UNSIGNED, edited smallint UNSIGNED, edits smallint UNSIGNED,  tag1 VARCHAR(24), tag2 VARCHAR(24), tag3 VARCHAR(24), tag4 VARCHAR(24), tag5 VARCHAR(24), 
number_of_tags smallint UNSIGNED, author VARCHAR(16), author_reputation smallint, author_avg_payout_1_week FLOAT(7,2), author_total_posts_1_week int UNSIGNED,
total_beneficiaries smallint UNSIGNED, total_votes smallint UNSIGNED, total_comments smallint UNSIGNED, percent_sbd int UNSIGNED, total_value FLOAT(7,2) UNSIGNED,
author_value FLOAT(7,2) UNSIGNED, curation_value FLOAT(7,2) UNSIGNED, beneficiary_value FLOAT(7,2) UNSIGNED, 
median_voter_1_hour_avg_curation_rewards FLOAT(7,2) UNSIGNED, top_voter_1_hour_avg_curation_rewards FLOAT(7,2) UNSIGNED,
4_min_value FLOAT(7,2) UNSIGNED, 5_min_value FLOAT(7,2) UNSIGNED, 6_min_value FLOAT(7,2) UNSIGNED, 7_min_value FLOAT(7,2) UNSIGNED,
8_min_value FLOAT(7,2) UNSIGNED, 9_min_value FLOAT(7,2) UNSIGNED, 10_min_value FLOAT(7,2) UNSIGNED, 15_min_value FLOAT(7,2) UNSIGNED,
20_min_value FLOAT(7,2) UNSIGNED, 25_min_value FLOAT(7,2) UNSIGNED, 30_min_value FLOAT UNSIGNED, 1_hour_value FLOAT(7,2) UNSIGNED,
12_hours_value FLOAT(7,2) UNSIGNED, 1_day_value FLOAT(7,2) UNSIGNED, 2_days_value FLOAT(7,2) UNSIGNED, 3_days_value FLOAT(7,2) UNSIGNED,
4_days_value FLOAT(7,2) UNSIGNED, 5_days_value FLOAT(7,2) UNSIGNED, 6_days_value FLOAT(7,2) UNSIGNED,
4_min_votes smallint UNSIGNED, 5_min_votes smallint UNSIGNED, 6_min_votes smallint UNSIGNED, 7_min_votes smallint UNSIGNED,
8_min_votes smallint, 9_min_votes smallint, 10_min_votes smallint UNSIGNED, 15_min_votes smallint UNSIGNED,
20_min_votes smallint UNSIGNED, 25_min_votes smallint UNSIGNED, 30_min_votes smallint UNSIGNED, 1_hour_votes smallint UNSIGNED,
12_hours_votes smallint UNSIGNED, 1_day_votes smallint UNSIGNED, 2_days_votes smallint UNSIGNED, 3_days_votes smallint UNSIGNED,
4_days_votes smallint UNSIGNED, 5_days_votes smallint UNSIGNED, 6_days_votes smallint UNSIGNED,
4_min_avg_vote_value FLOAT(7,2) UNSIGNED, 5_min_avg_vote_value FLOAT(7,2) UNSIGNED, 6_min_avg_vote_value FLOAT(7,2) UNSIGNED, 7_min_avg_vote_value FLOAT(7,2) UNSIGNED,
8_min_avg_vote_value FLOAT(7,2) UNSIGNED, 9_min_avg_vote_value FLOAT(7,2) UNSIGNED, 10_min_avg_vote_value FLOAT(7,2) UNSIGNED, 15_min_avg_vote_value FLOAT(7,2) UNSIGNED,
20_min_avg_vote_value FLOAT(7,2) UNSIGNED, 25_min_avg_vote_value FLOAT(7,2) UNSIGNED, 30_min_avg_vote_value FLOAT UNSIGNED, 1_hour_avg_vote_value FLOAT(7,2) UNSIGNED,
12_hours_avg_vote_value FLOAT(7,2) UNSIGNED, 1_day_avg_vote_value FLOAT(7,2) UNSIGNED, 2_days_avg_vote_value FLOAT(7,2) UNSIGNED, 3_days_avg_vote_value FLOAT(7,2) UNSIGNED,
4_days_avg_vote_value FLOAT(7,2) UNSIGNED, 5_days_avg_vote_value FLOAT(7,2) UNSIGNED, 6_days_avg_vote_value FLOAT(7,2) UNSIGNED,
4_min_median_voter VARCHAR(16), 5_min_median_voter VARCHAR(16), 6_min_median_voter VARCHAR(16), 7_min_median_voter VARCHAR(16),
8_min_median_voter VARCHAR(16), 9_min_median_voter VARCHAR(16), 10_min_median_voter VARCHAR(16), 15_min_median_voter VARCHAR(16),
20_min_median_voter VARCHAR(16), 25_min_median_voter VARCHAR(16), 30_min_median_voter VARCHAR(16), 1_hour_median_voter VARCHAR(16),
12_hours_median_voter VARCHAR(16), 1_day_median_voter VARCHAR(16), 2_days_median_voter VARCHAR(16), 3_days_median_voter VARCHAR(16),
4_days_median_voter VARCHAR(16), 5_days_median_voter VARCHAR(16), 6_days_median_voter VARCHAR(16),
4_min_top_voter VARCHAR(16), 5_min_top_voter VARCHAR(16), 6_min_top_voter VARCHAR(16), 7_min_top_voter VARCHAR(16),
8_min_top_voter VARCHAR(16), 9_min_top_voter VARCHAR(16), 10_min_top_voter VARCHAR(16), 15_min_top_voter VARCHAR(16),
20_min_top_voter VARCHAR(16), 25_min_top_voter VARCHAR(16), 30_min_top_voter VARCHAR(16), 1_hour_top_voter VAR

















NonExistentTimeError






CHAR(16),
12_hours_top_voter VARCHAR(16), 1_day_top_voter VARCHAR(16), 2_days_top_voter VARCHAR(16), 3_days_top_voter VARCHAR(16),
4_days_top_voter VARCHAR(16), 5_days_top_voter VARCHAR(16), 6_days_top_voter VARCHAR(16), block int UNSIGNED, id int UNSIGNED PRIMARY KEY)""")
self.cursor.execute(f"""INSERT INTO TrainingData(title, link, title_word_count, body_word_count, avg_sentence_length, 
        body_paragraph_count, avg_paragraph_length, img_count, words_per_img,
        sentence_length_variance, steem_word_count, crypto_word_count, hive_word_count,
        percent_steem_words, steem_words_per_paragraph, primary_language, spelling_errors, 
        spelling_percent, spelling_errors_per_paragraph, spelling_errors_per_sentence, created, day, hour, edited, edits, 
        tag1, tag2, tag3, tag4, tag5, number_of_tags, author, author_reputation, author_avg_payout_1_week, author_total_posts_1_week,
        total_beneficiaries, total_votes, total_comments, percent_sbd, total_value,
        author_value, curation_value, beneficiary_value, 
        median_voter_1_hour_avg_curation_rewards, top_voter_1_hour_avg_curation_rewards,
        4_min_value, 5_min_value, 6_min_value, 7_min_value,
        8_min_value, 9_min_value, 10_min_value, 15_min_value,
        20_min_value, 25_min_value, 30_min_value, 1_hour_value,
        12_hours_value, 1_day_value, 2_days_value, 3_days_value,
        4_days_value, 5_days_value, 6_days_value,
        4_min_rshares, 5_min_rshares, 6_min_rshares, 7_min_rshares,
        8_min_rshares, 9_min_rshares, 10_min_rshares, 15_min_rshares,
        20_min_rshares, 25_min_rshares, 30_min_rshares, 1_hour_rshares,
        12_hours_rshares, 1_day_rshares, 2_days_rshares, 3_days_rshares,
        4_days_rshares, 5_days_rshares, 6_days_rshares,
        4_min_votes, 5_min_votes, 6_min_votes, 7_min_votes,
        8_min_votes, 9_min_votes, 10_min_votes, 15_min_votes,
        20_min_votes, 25_min_votes, 30_min_votes, 1_hour_votes,
        12_hours_votes, 1_day_votes, 2_days_votes, 3_days_votes,
        4_days_votes, 5_days_votes, 6_days_votes,
        4_min_avg_vote_value, 5_min_avg_vote_value, 6_min_avg_vote_value, 7_min_avg_vote_value,
        8_min_avg_vote_value, 9_min_avg_vote_value, 10_min_avg_vote_value, 15_min_avg_vote_value,
        20_min_avg_vote_value, 25_min_avg_vote_value, 30_min_avg_vote_value, 1_hour_avg_vote_value,
        12_hours_avg_vote_value, 1_day_avg_vote_value, 2_days_avg_vote_value, 3_days_avg_vote_value,
        4_days_avg_vote_value, 5_days_avg_vote_value, 6_days_avg_vote_value,
        4_min_median_voter, 5_min_median_voter, 6_min_median_voter, 7_min_median_voter,
        8_min_median_voter, 9_min_median_voter, 10_min_median_voter, 15_min_median_voter,
        20_min_median_voter, 25_min_median_voter, 30_min_median_voter, 1_hour_median_voter,
        12_hours_median_voter, 1_day_median_voter, 2_days_median_voter, 3_days_median_voter,
        4_days_median_voter, 5_days_median_voter, 6_days_median_voter,
        4_min_top_voter, 5_min_top_voter, 6_min_top_voter, 7_min_top_voter,
        8_min_top_voter, 9_min_top_voter, 10_min_top_voter, 15_min_top_voter,
        20_min_top_voter, 25_min_top_voter, 30_min_top_voter, 1_hour_top_voter,
        12_hours_top_voter, 1_day_top_voter, 2_days_top_voter, 3_days_top_voter,
        4_days_top_voter, 5_days_top_voter, 6_days_top_voter, 
        4_min_top_voter_value, 5_min_top_voter_value, 6_min_top_voter_value, 7_min_top_voter_value,
        8_min_top_voter_value, 9_min_top_voter_value, 10_min_top_voter_value, 15_min_top_voter_value,
        20_min_top_voter_value, 25_min_top_voter_value, 30_min_top_voter_value, 1_hour_top_voter_value,
        12_hours_top_voter_value, 1_day_top_voter_value, 2_days_top_voter_value, 3_days_top_voter_value,
        4_days_top_voter_value, 5_days_top_voter_value, 6_days_top_voter_value,
        4_min_top_voter_rshares, 5_min_top_voter_rshares, 6_min_top_voter_rshares, 7_min_top_voter_rshares,
        8_min_top_voter_rshares, 9_min_top_voter_rshares, 10_min_top_voter_rshares, 15_min_top_voter_rshares,
        20_min_top_voter_rshares, 25_min_top_voter_rshares, 30_min_top_voter_rshares, 1_hour_top_voter_rshares,
        12_hours_top_voter_rshares, 1_day_top_voter_rshares, 2_days_top_voter_rshares, 3_days_top_voter_rshares,
        4_days_top_voter_rshares, 5_days_top_voter_rshares, 6_days_top_voter_rshares, 
        4_min_top_voter_percent, 5_min_top_voter_percent, 6_min_top_voter_percent, 7_min_top_voter_percent,
        8_min_top_voter_percent, 9_min_top_voter_percent, 10_min_top_voter_percent, 15_min_top_voter_percent,
        20_min_top_voter_percent, 25_min_top_voter_percent, 30_min_top_voter_percent, 1_hour_top_voter_percent,
        12_hours_top_voter_percent, 1_day_top_voter_percent, 2_days_top_voter_percent, 3_days_top_voter_percent,
        4_days_top_voter_percent, 5_days_top_voter_percent, 6_days_top_voter_percent, 
        4_min_median_voter_value, 5_min_median_voter_value, 6_min_median_voter_value, 7_min_median_voter_value,
        8_min_median_voter_value, 9_min_median_voter_value, 10_min_median_voter_value, 15_min_median_voter_value,
        20_min_median_voter_value, 25_min_median_voter_value, 30_min_median_voter_value, 1_hour_median_voter_value,
        12_hours_median_voter_value, 1_day_median_voter_value, 2_days_median_voter_value, 3_days_median_voter_value,
        4_days_median_voter_value, 5_days_medianfrom Downloading.AccountInfo import getPrice_voter_value, 6_days_medianvoter_value,
        4_min_median_voter_rshares, 5_min_median_voter_rshares, 6_min_median_voter_rshares, 7_min_median_voter_rshares,
        8_min_median_voter_rshares, 9_min_median_voter_rshares, 10_min_medianvoter_rshares, 15_min_median_voter_rshares,
        20_min_median_voter_rshares, 25_min_median_voter_rshares, 30_min_median_voter_rshares, 1_hour_median_voter_rshares,
        12_hours_median_voter_rshares, 1_day_median_voter_rshares, 2_days_median_voter_rshares, 3_days_median_voter_rshares,
        4_days_median_voter_rshares, 5_days_median_voter_rshares, 6_days_median_voter_rshares, 
        4_min_median_voter_percent, 5_min_median_voter_percent, 6_min_median_voter_percent, 7_min_median_voter_percent,
        8_min_median_voter_percent, 9_min_median_voter_percent, 10_min_median_voter_percent, 15_min_median_voter_percent,
        20_min_median_voter_percent, 25_min_median_voter_percent, 30_min_median_voter_percent, 1_hour_median_voter_percent,
        12_hours_median_voter_percent, 1_day_median_voter_percent, 2_days_median_voter_percent, 3_days_median_voter_percent,
        4_days_median_voter_percent, 5_days_median_voter_percent, 6_days_median_voter_percent, 
        4_min_total_comments, 5_min_total_comments, 6_min_total_comments, 7_min_total_comments, 8_min_total_comments,
        9_min_total_comments, 10_min_total_comments, 15_min_total_comments, 20_min_total_comments, 25_min_total_comments,
        30_min_total_comments, 1_hour_total_comments, 12_hours_total_comments, 1_day_total_comments, 2_days_total_comments,
        3_days_total_comments, 4_days_total_comments, 5_days_total_comments, 6_days_total_comments,
        4_min_mean_comment_value, 5_min_mean_comment_value, 6_min_mean_comment_value, 7_min_mean_comment_value, 8_min_mean_comment_value,
        9_min_mean_comment_value, 10_min_mean_comment_value, 15_min_mean_comment_value, 20_min_mean_comment_value, 25_min_mean_comment_value,
        30_min_mean_comment_value, 1_hour_mean_comment_value, 12_hours_mean_comment_value, 1_day_mean_comment_value, 2_days_mean_comment_value,
        3_days_mean_comment_value, 4_days_mean_comment_value, 5_days_mean_comment_value, 6_days_mean_comment_value,
        4_min_mean_comment_votes, 5_min_mean_comment_votes, 6_min_mean_comment_votes, 7_min_mean_comment_votes, 8_min_mean_comment_votes,
        9_min_mean_comment_votes, 10_min_mean_comment_votes, 15_min_mean_comment_votes, 20_min_mean_comment_votes, 25_min_mean_comment_votes,
        30_min_mean_comment_votes, 1_hour_mean_comment_votes, 12_hours_mean_comment_votes, 1_day_mean_comment_votes, 2_days_mean_comment_votes,
        3_days_mean_comment_votes, 4_days_mean_comment_votes, 5_days_mean_comment_votes, 6_days_mean_comment_votes,
        4_min_max_commenter_reputation, 5_min_max_commenter_reputation, 6_min_max_commenter_reputation, 7_min_max_commenter_reputation,
        8_min_max_commenter_reputation, 9_min_max_commenter_reputation, 10_min_max_commenter_reputation, 15_min_max_commenter_reputation,
        20_min_max_commenter_reputation, 25_min_max_commenter_reputation, 30_min_max_commenter_reputation, 1_hour_max_commenter_reputation,
        12_hours_max_commenter_reputation, 1_day_maxgetPrice('steem', )_commenter_reputation, 2_days_max_commenter_reputation, 3_days_max_commenter_reputation,
        4_days_max_commenter_reputation, 5_days_max_commenter_reputation, 6_days_max_commenter_reputation,
        4_min_top_value_comment, 5_min_top_value_comment, 6_min_top_value_comment, 7_min_top_value_comment, 8_min_top_value_comment,
        9_min_top_value_comment, 10_min_top_value_comment, 15_min_top_value_comment, 20_min_top_value_comment, 25_min_top_value_comment,
        30_min_top_value_comment, 1_hour_top_value_comment, 12_hours_top_value_comment, 1_day_top_value_comment, 2_days_top_value_comment,
        3_days_top_value_comment, 4_days_top_value_comment, 5_days_top_value_comment, 6_days_top_value_comment, block, id) VALUES (
        {article["Title"]}, "{article['Link']}", "{article['Title Word Count']}", 
        "{article['Body Word Count']}", "{article['Average Sentence Length']}", "{article['Number of Body Paragraphs']}", 
        "{article['Average Paragraph Length']}", "{article['Total Images']}", "{article['Words per Image']}", "{article['Sentence Length Variance']}",
        "{article['Number of Steem Words']}", "{article['Number of Crypto Words']}", "{article['Number of Hive Words']}", 
        "{article['Percent of Steem Words']}", "{article['Steem Words per Paragraph']}",
        "{article['Primary Language']}", "{article['Spelling Errors']}", "{article['Spelling Percent']}", "{article['Spelling Errors per Paragraph']}", 
        "{article['Spelling Errors per Sentence']}", "{article['Created']}", "{article['Day']}", "{article['Hour']}", "{article['Edited']}", "{article['Edits']}", "{article['Tag1']}",
        "{article['Tag2']}", "{article['Tag3']}", "{article['Tag4']}", "{article['Tag5']}", "{article['Numbe
























r of Tags']}",
        "{article['Author']}", "{article['Author Reputation']}", "{article['Average 1 week Author Rewards']}", "{article['Total Posts in 1 week']}","{article['Total Beneficiaries']}", 
        "{article['Total Votes']}", "{article['Total Comments']}","{article['Percent SBD']}", "{article['Total Value']}", "{article['Author Value']}", "{article['Curation Value']}",
        "{article['Beneficiary Value']}", "{article['1 hour median voter avg week rewards']}", "{article['1 hour top voter avg week rewards']}", 
        "{article['4_min_value']}", "{article['5_min_value']}", "{article['6_min_value']}", "{article['7_min_value']}", "{article['8_min_value']}",
        "{article['9_min_value']}", "{article['10_min_value']}", "{article['15_min_value']}", "{article['20_min_value']}", "{article['25_min_value']}",
        "{article['30_min_value']}", "{article['1_hour_value']}", "{article['12_hours_value']}", "{article['1_day_value']}", "{article['2_days_value']}",
        "{article['3_days_value']}",  "{article['4_days_value']}",  "{article['5_days_value']}", "{article['6_days_value']}",
        "{article['4_min_rshares']}", "{article['5_min_rshares']}", "{article['6_min_rshares']}", "{article['7_min_rshares']}", "{article['8_min_rshares']}",
        "{article['9_min_rshares']}", "{article['10_min_rshares']}", "{article['15_min_rshares']}", "{article['20_min_rshares']}", "{article['25_min_rshares']}",
        "{article['30_min_rshares']}", "{article['1_hour_rshares']}", "{article['12_hours_rshares']}", "{article['1_day_rshares']}", "{article['2_days_rshares']}",
        "{article['3_days_rshares']}",  "{article['4_days_rshares']}",  "{article['5_days_rshares']}", "{article['6_days_rshares']}",
        "{article['4_min_votes']}", "{article['5_min_votes']}", "{article['6_min_votes']}", "{article['7_min_votes']}", "{article['8_min_votes']}",
        "{article['9_min_votes']}", "{article['10_min_votes']}", "{article['15_min_votes']}", "{article['20_min_votes']}", "{article['25_min_votes']}",
        "{article['30_min_votes']}", "{article['1_hour_votes']}", "{article['12_hours_votes']}", "{article['1_day_votes']}", "{article['2_days_votes']}",
        "{article['3_days_votes']}",  "{article['4_days_votes']}",  "{article['5_days_votes']}", "{article['6_days_votes']}",
        "{article['4_min_avg_vote_val']}", "{article['5_min_avg_vote_val']}", "{article['6_min_avg_vote_val']}", "{article['7_min_avg_vote_val']}", "{article['8_min_avg_vote_val']}",
        "{article['9_min_avg_vote_val']}", "{article['10_min_avg_vote_val']}", "{article['15_min_avg_vote_val']}", "{article['20_min_avg_vote_val']}", "{article['25_min_avg_vote_val']}",
        "{article['30_min_avg_vote_val']}", "{article['1_hour_avg_vote_val']}", "{article['12_hours_avg_vote_val']}", "{article['1_day_avg_vote_val']}", "{article['2_days_avg_vote_val']}",
        "{article['3_days_avg_vote_val']}",  "{article['4_days_avg_vote_val']}",  "{article['5_days_avg_vote_val']}", "{article['6_days_avg_vote_val']}",
        "{article['4_min_median_voter']}", "{article['5_min_median_voter']}", "{article['6_min_median_voter']}", "{article['7_min_median_voter']}", "{article['8_min_median_voter']}",
        "{article['9_min_median_voter']}", "{article['10_min_median_voter']}", "{article['15_min_median_voter']}", "{article['20_min_median_voter']}", "{article['25_min_median_voter']}",
        "{article['30_min_median_voter']}", "{article['1_hour_median_voter']}", "{article['12_hours_median_voter']}", "{article['1_day_median_voter']}", "{article['2_days_median_voter']}",
        "{article['3_days_median_voter']}",  "{article['4_days_median_voter']}",  "{article['5_days_median_voter']}", "{article['6_days_median_voter']}",
        "{article['4_min_top_voter']}", "{article['5_min_top_voter']}", "{article['6_min_top_voter']}", "{article['7_min_top_voter']}", "{article['8_min_top_voter']}",
        "{article['9_min_top_voter']}", "{article['10_min_top_voter']}", "{article['15_min_top_voter']}", "{article['20_min_top_voter']}", "{article['25_min_top_voter']}",
        "{article['30_min_top_voter']}", "{article['1_hour_top_voter']}", "{article['12_hours_top_voter']}", "{article['1_day_top_voter']}", "{article['2_days_top_voter']}",
        "{article['3_days_top_voter']}",  "{article['4_days_top_voter']}",  "{article['5_days_top_voter']}", "{article['6_days_top_voter']}", 
        "{article['4_min_top_voter_value']}", "{article['5_min_top_voter_value']}", "{article['6_min_top_voter_value']}", "{article['7_min_top_voter_value']}", 
        "{article['8_min_top_voter_value']}", "{article['9_min_top_voter_value']}", "{article['10_min_top_voter_value']}", "{article['15_min_top_voter_value']}", 
        "{article['20_min_top_voter_value']}", "{article['25_min_top_voter_value']}", "{article['30_min_top_voter_value']}", "{article['1_hour_top_voter_value']}", 
        "{article['12_hours_top_voter_value']}", "{article['1_day_top_voter_value']}", "{article['2_days_top_voter_value']}",
        "{article['3_days_top_voter_value']}",  "{article['4_days_top_voter_value']}",  "{article['5_days_top_voter_value']}", "{article['6_days_top_voter_value']}",
        "{article['4_min_top_voter_rshares']}", "{article['515_min_top_voter_rshares']}", "{article['6_min_top_voter_rshares']}", "{article['7_min_top_voter_rshares']}", 
        "{article['8_min_top_voter_rshares']}", "{article['9_min_top_voter_rshares']}", "{article['10_min_top_voter_rshares']}", "{article['15_min_top_voter_rshares']}", 
        "{article['20_min_top_voter_rshares']}", "{article['25_min_top_voter_rshares']}", "{article['30_min_top_voter_rshares']}", "{article['1_hour_top_voter_rshares']}", 
        "{article['12_hours_top_voter_rshares']}", "{article['1_day_top_voter_rshares']}", "{article['2_days_top_voter_rshares']}", "{article['3_days_top_voter_rshares']}",  
        "{article['4_days_top_voter_rshares']}",  "{article['5_days_top_voter_rshares']}", "{article['6_days_top_voter_rshares']}",
        "{article['4_min_top_voter_percent']}", "{article['5_min_top_voter_percent']}", "{article['6_min_top_voter_percent']}", "{article['7_min_top_voter_percent']}", 
        "{article['8_min_top_voter_percent']}", "{article['9_min_top_voter_percent']}", "{article['10_min_top_voter_percent']}", "{article['15_min_top_voter_percent']}", 
        "{article['20_min_top_voter_percent']}", "{article['25_min_top_voter_percent']}", "{article['30_min_top_voter_percent']}", "{article['1_hour_top_voter_percent']}", 
        "{article['12_hours_top_voter_percent']}", "{article['1_day_top_voter_percent']}", "{article['2_days_top_voter_percent']}", "{article['3_days_top_voter_percent']}",  
        "{article['4_days_top_voter_percent']}",  "{article['5_days_top_voter_percent']}", "{article['6_days_top_voter_percent']}",
        "{article['4_min_median_voter_value']}", "{article['5_min_median_voter_value']}", "{article['6_min_median_voter_value']}", "{article['7_min_median_voter_value']}", 
        "{article['8_min_median_voter_value']}", "{article['9_min_median_voter_value']}", "{article['10_min_median_voter_value']}", "{article['15_min_median_voter_value']}", 
        "{article['20_min_median_voter_value']}", "{article['25_min_median_voter_value']}", "{article['30_min_median_voter_value']}", "{article['1_hour_median_voter_value']}", 
        "{article['12_hours_median_voter_value']}", "{article['1_day_median_voter_value']}", "{article['2_days_median_voter_value']}",
        "{article['3_days_median_voter_value']}",  "{article['4_days_median_voter_value']}",  "{article['5_days_median_voter_value']}", "{article['6_days_median_voter_value']}",
        "{article['4_min_median_voter_rshares']}", "{article['5_min_median_voter_rshares']}", "{article['6_min_median_voter_rshares']}", "{article['7_min_median_voter_rshares']}", 
        "{article['8_min_median_voter_rshares']}", "{article[e
_'9_min_median_voter_rshares']}", "{article['10_min_median_voter_rshares']}", "{article['15_min_median_voter_rshares']}", 
        "{article['20_min_median_voter_rshares']}", "{article['25_min_median_voter_rshares']}", "{article['30_min_median_voter_rshares']}", "{article['1_hour_median_voter_rshares']}", 
        "{article['12_hours_median_voter_rshares']}", "{article['1_day_median_voter_rshares']}", "{article['2_days_median_voter_rshares']}", "{article['3_days_median_voter_rshares']}",  
        "{article['4_days_median_voter_rshares']}",  "{article['5_days_median_voter_rshares']}", "{article['6_days_median_voter_rshares']}",
        "{article['4_min_median_voter_percent']}", "{article['5_min_median_voter_percent']}", "{article['6_min_median_voter_percent']}", "{article['7_min_median_voter_percent']}", 
        "{article['8_min_median_voter_percent']}", "{article['9_min_median_voter_percent']}", "{article['10_min_median_voter_percent']}", "{article['15_min_median_voter_percent']}", 
        "{article['20_min_median_voter_percent']}", "{article['25_min_median_voter_percent']}", "{article['30_min_median_voter_percent']}", "{article['1_hour_median_voter_percent']}", 
        "{article['12_hours_median_voter_percent']}", "{article['1_day_median_voter_percent']}", "{article['2_days_median_voter_percent']}", "{article['3_days_median_voter_percent']}",  
        "{article['4_days_median_voter_percent']}",  "{article['5_days_median_voter_percent']}", "{article['6_days_median_voter_percent']}",
        "{article['Block']}", "{ID}") 
        ON DUPLICATE KEY UPDATE title{article["Title"]}, title_word_count = "{article['Title Word Count']}", body_word_count = "{article['Body Word Count']}", 
        spelling_errors="{article['Spelling Errors']}", spelling_percent="{article['Spelling Percent']}", edits="{article['Edits']}", 
        number_of_tags="{article['Number of Tags']}";""")
        self.db.commit()
'''

MLP_LABELS = ['id', 'block', 'title', 'link', 'title_word_count', 'body_word_count', 'avg_sentence_length',
        'body_paragraph_count', 'avg_paragraph_length', 'img_count', 'words_per_img',
        'sentence_length_variance', 'primary_language', 'spelling_errors',
        'spelling_percent', 'spelling_errors_per_paragraph', 'spelling_errors_per_sentence', 'created', 'day', 'hour', 'edited',
        'tag1', 'tag2', 'tag3', 'tag4', 'tag5', 'number_of_tags', 'author', 'author_reputation', 'author_avg_payout_1_week', 'author_total_posts_1_week',
        'total_beneficiaries', 'total_votes', 'percent_sbd', 'percent_burned', 'total_value',
        'author_value', 'curation_value', 'beneficiary_value',
        'median_voter_1_hour_avg_curation_rewards', 'top_voter_1_hour_avg_curation_rewards', '5_min_value', '1_hour_value', 'value_change',
        '5_min_votes', '1_hour_votes', 'votes_change', 'resteems', 'followers']

class SSQL():
    def __init__(self):
        self.db = self.run_database()
        self.cursor = self.db.cursor(buffered=True)

    def run_database(self, host:str='localhost', user:str='cmp2020', passwd:str='Schubert1-31', db:str='TrainingData'):
        self.db = mysql.connector.connect(
            host=host,  # change if using server
            user=user,
            passwd=passwd,
            database=db
        )
        return self.db

    def get_first_index(self):
        sworld = self.get_data('SteemWorldData')
        blocktrain_training = self.get_data('TrainingBlockchainData')
        last = sworld.iloc[-1]['id']
        not_in = blocktrain_training[blocktrain_training['id']>last]
        return len(blocktrain_training) - len(not_in)

    def get_price(self, date):
        prices = self.get_data('historic_prices')
        prices['date'] = pd.to_datetime(prices['date'])
        prices = prices[prices['date']==date]
        if len(prices) == 0:
            return None
        else:
            return prices.iloc[0]['price']

    def get_data_not_matching(self):
        block = self.get_data('TrainingBlockchainData')
        sworld = self.get_data('SteemWorldData')
        mask = block['id'].isin(sworld['id'])
        block = block[~mask]
        return block

    def get_live_posts(self, ids):
        id_string = ','.join(str(x) for x in ids)
        query = f'SELECT * FROM LivePosts WHERE id IN ({id_string});'
        self.cursor.execute(query)
        articles = self.cursor.fetchall()
        self.cursor.execute("""SHOW columns FROM LivePosts""")
        columns = []
        for column in self.cursor.fetchall():
            columns.append(column[0])
        df = pd.DataFrame(data=articles, columns=columns)
        df['created'] = pd.to_datetime(df['created'])
        if 'created' in df.columns:
            df['created'] = df['created'].dt.tz_localize('UTC')
            df['created'] = df['created'].dt.strftime('%Y-%m-%dT%H:%M:%S')
        return df

    def remove_live_post(self, id):
        query = f'DELETE FROM LivePosts WHERE id = {id};'
        self.cursor.execute(query)
        self.db.commit()

    def insert_training_articles(self, table, articles, return_n=True):
        self.cursor.execute(f'SELECT COUNT(*) FROM {table};')
        len_b4_ins = self.cursor.fetchone()
        for article in articles:
            article['inserted'] = datetime.datetime.now()
            QUERY1 = f"""INSERT INTO {table}("""
            keys = list(article.keys())
            for z in range(len(keys)):
                label = keys[z]
                if z < len(keys) - 1:
                    QUERY1 += f"{label}, "
                else:
                    QUERY1 += f"{label})"
            QUERY2 = f""" VALUES ("""
            QUERY3 = f""" ON DUPLICATE KEY UPDATE"""
            for x in range(len(keys)):
                label = keys[x]
                if label == "title" or label == "body":
                    QUERY2 += f""" '''{article[f'{label}']}''',"""
                    QUERY3 += f""" {label}='''{article[f'{label}']}''',"""
                elif x == len(keys) - 1:
                    QUERY2 += f""" "{article[f'{label}']}")"""
                    QUERY3 += f""" {label}="{article[f'{label}']}";"""
                else:
                    QUERY2 += f""" "{article[f'{label}']}","""
                    QUERY3 += f""" {label}="{article[f'{label}']}","""

            QUERY = QUERY1 + QUERY2 + QUERY3
            try:
                self.cursor.execute(QUERY)
            except Exception as e:
                print(e)
        try:
            self.db.commit()
        except Exception as e:
            print(e)

        self.cursor.execute(f'SELECT COUNT(*) FROM {table};')
        len_af_ins = self.cursor.fetchone()
        n = len_af_ins[0] - len_b4_ins[0]
        print(f'{n} samples inserted into {table}')
        if return_n == True:
            return self.get_n_data(table, n)

    def insert_price(self, date, price):
        self.cursor.execute(f"""INSERT INTO historic_prices (date, price) VALUES("{date}", "{price}");""")
        self.db.commit()
        return self.get_data('historic_prices')

    def get_data(self, table:str):
        self.cursor.execute(f"""SELECT * FROM {table}""")
        articles = self.cursor.fetchall()
        self.cursor.execute(f"""SHOW columns FROM {table}""")
        columns = []
        for column in self.cursor.fetchall():
            columns.append(column[0])
        df = pd.DataFrame(data=articles, columns=columns)
        if 'created' in df.columns:
            df['created'] = df['created'].dt.tz_localize('UTC')
            df['created'] = df['created'].dt.strftime('%Y-%m-%dT%H:%M:%S')
        if 'payout_date' in df.columns:
            df['payout_date'] = df['payout_date'].dt.tz_localize('UTC')
            df['payout_date'] = df['payout_date'].dt.strftime('%Y-%m-%dT%H:%M:%S')
            df['payout_date'] = pd.to_datetime(df['payout_date'])
        return df

    def get_n_data(self, table:str, n):
        self.cursor.execute(f"SELECT * FROM {table} ORDER BY id DESC LIMIT {n}")
        articles = self.cursor.fetchall()
        self.cursor.execute(f"""SHOW columns FROM {table}""")
        columns = []
        for column in self.cursor.fetchall():
            columns.append(column[0])
        df = pd.DataFrame(data=articles, columns=columns)
        return df

    def delete_last_n_articles(self, table:str='TrainingData2', n:int=1):
        df = self.get_n_data(table, n)
        ids = []
        for i in range(n):
            ids.append(df.iloc[i]['id'])

        query = f'DELETE FROM {table} WHERE id in ('
        for x in range(len(ids)):
            if x != len(ids) - 1:
                query += f'{ids[x]}, '
            else:
                query += f'{ids[x]})'
        self.cursor.execute(query)
        self.db.commit()
        return self.get_n_data(table, 1)

    def clear_table(self, table):
        Answer = int(input("Are you sure you want to clear the TrainingData? This will delete everything in the tables! Type 1 to confirm"))
        if Answer == 1:
            self.cursor.execute(f"DELETE FROM {table}")
            self.db.commit()

#columns to addEurope/London
# Avg author rewards in last month
# Avg 60_min, 360_min, 720_min median and top voter rewards in last week
# Post total resteems 60_min, 360_min, 720_min
# Followers gained in previous month (smallint)
# Author account age

