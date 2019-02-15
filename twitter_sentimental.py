#!/usr/bin/python
# -*- coding: utf-8 -*-
import os

import couchdb
import sys
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import json
import time

db_url = 'http://admin:admin@127.0.0.1:5984'
db_list = ['sydney', 'melbourne', 'brisbane', 'perth', 'adelaide', 'gold_coast', 'newcastle', 'canberra', 'wollongong',
           'sunshine_coast', 'hobart', 'geelong', 'townsville', 'cairns', 'toowoomba', 'darwin', 'ballarat']
# db_list = ['ballarat', 'hobart', 'cairns']


# Get View Method
def get_tweets(db_name):
    server = couchdb.Server(url=db_url)
    db = server[db_name]
    return db.view('twitter/get_tweets')
    # return db


# Write JSON File
def save_json(file_name, data):
    with open(file_name, 'w+') as f:
        json.dump(data, f)

# Sentiment Analysis (all by city)
positive = 0
neutral = 1
negative = 2
analyzer = SentimentIntensityAnalyzer()
# By time (5am-8am, 8am-11am, 11am-2pm, 2pm-5pm, 5pm-8pm, 8pm-12pm, 12pm-2am, 2am-5am)
# By date (Mon, Tue, Wed, Thur, Fri, Sat, Sun)
# By season (Sep-Nov, Dec-Feb, Mar-May, Jun-Aug)
# By event (Easter Holiday, Apr 11-26)
# By city
sentiment_city_t = {}
sentiment_city_w = {}
sentiment_city_s = {}
sentiment_city_e = {}
sentiment_city = {}
for i in range(len(db_list)):
    view_t = get_tweets(db_name=db_list[i] + "_no_repeat")
    print(view_t)
    
    sentiment_t = [[0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0]]
    sentiment_w = [[0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0]]
    sentiment_s = [[0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0]]
    sentiment_e = [[0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0],
                   [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0]]
    sentiment_c = [0, 0, 0]
    for tweet in view_t:
    # for key in view_t:
        # tweet = view_t[key]
        tweet_text = tweet.value[0]
        tweet_time = int(tweet.value[1].split(" ")[3].split(":")[0])
        tweet_weekday = tweet.value[1].split(" ")[0]
        tweet_month = tweet.value[1].split(" ")[1]
        tweet_date = tweet.value[1].split(" ")[1] + " " + tweet.value[1].split(" ")[2]
        score = analyzer.polarity_scores(tweet_text).get("compound")
        # classify by sentiment
        if score >= 0.1:
            index_inner = positive
        elif score <= -0.1:
            index_inner = negative
        else:
            index_inner = neutral
        # classify by time
        if 2 <= tweet_time < 5:
            index_outer_t = 0
        elif 5 <= tweet_time < 8:
            index_outer_t = 1
        elif 8 <= tweet_time < 11:
            index_outer_t = 2
        elif 11 <= tweet_time < 14:
            index_outer_t = 3
        elif 14 <= tweet_time < 17:
            index_outer_t = 4
        elif 17 <= tweet_time < 20:
            index_outer_t = 5
        elif 20 <= tweet_time < 23:
            index_outer_t = 6
        else:
            index_outer_t = 7
        # classify by date
        if tweet_weekday == "Mon":
            index_outer_w = 0
        elif tweet_weekday == "Tue":
            index_outer_w = 1
        elif tweet_weekday == "Wed":
            index_outer_w = 2
        elif tweet_weekday == "Thu":
            index_outer_w = 3
        elif tweet_weekday == "Fri":
            index_outer_w = 4
        elif tweet_weekday == "Sat":
            index_outer_w = 5
        else:
            index_outer_w = 6
    sentiment_city_t[db_list[i]] = sentiment_t
    sentiment_city_w[db_list[i]] = sentiment_w
    sentiment_city[db_list[i]] = sentiment_c
# print(sentiment_city_t)
# print(sentiment_city_w)
# print(sentiment_city_s)
# print(sentiment_city_e)
# print(sentiment_city)
# Save in JSON File
save_json("/Users/xuanzhang/FIT5147SourceCode/web/static/sentiment_by_time.json", sentiment_city_t)
save_json("/Users/xuanzhang/FIT5147SourceCode/web/static/sentiment_by_weekday.json", sentiment_city_w)
save_json("/Users/xuanzhang/FIT5147SourceCode/web/static/sentiment_by_city.json", sentiment_city)
print("Latest Update Time: " + time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))

# def save_result(db_name):
#     server = couchdb.Server(url=db_url)
#     try:
#         db_server.create(db_name)
#     except couchdb.http.PreconditionFailed:
#         db_save = db_server[db_name]


# # test
# view = get_tweets("ballarat")
# count = 0
# for i in view:
#     print(i.value[1])
#     if count > 10:
#         break
#     count += 1
