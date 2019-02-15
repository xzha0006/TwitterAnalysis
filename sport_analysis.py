# -*- coding:utf-8 -*-
import couchdb
import langdetect
import sys
from couchdb.design import ViewDefinition
import pandas as pd
import numpy as np
import re
from langdetect import detect,DetectorFactory
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import json
import os
db_url = 'http://admin:admin@127.0.0.1:5984'
server = couchdb.Server(db_url)
aurin_db = server['aurin_sports']
city = ['ballarat', 'melbourne','geelong']
# city_ids=['01864a8a64df9dc4','01552cf31a0d108e','0077e693247f71d8']
view = ViewDefinition('aurin', 'get_yards', '''function(doc){emit(doc.properties.objectid,doc)}''')
view.sync(aurin_db)


def haversine_np(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    km = 6367 * c * 1000
    return km


def text_pre_process_for_lang(doc_df):
    regex = re.compile(r'[^\w\s\.\,\-]|(http|ftp|https):\/\/[\w\-_]+(\.[\w\-_]+)+([\w\-\.,@?^=%&amp;:/~\+#]*[\w\-\@?^=%&amp;/~\+#])?')
    return list(map(lambda x: regex.sub('', x.strip().replace('&amp', '')), list(doc_df.text)))


def get_sent(text, analyzer):
    positive = 1
    neutral = 0
    negative = -1
    score = analyzer.polarity_scores(text).get("compound")
    # classify by sentiment
    if score >= 0.1:
        index_inner = positive
    elif score <= -0.1:
        index_inner = negative
    else:
        index_inner = neutral
    return index_inner

yards = []
for yard in aurin_db.view('aurin/get_yards'):
    yards.append(yard.value['properties'])

yards_df=pd.DataFrame(yards).drop_duplicates()
yards_df.columns = list(map(lambda x: x.strip(), list(yards_df.columns)))
yards_df = yards_df[['sportsplayed', 'longitude', 'latitude', 'objectid']]
yards_df.index = yards_df.objectid


tweets=[]
for c in city:
    city_db = server[c+'_no_repeat']
    ll_view = ViewDefinition('twitter', 'get_tweets_with_ll', '''function(doc){if(doc.geo!=null){emit(doc.id_str,[doc.text,doc.geo.coordinates[0],doc.geo.coordinates[1]])}}''')
    ll_view.sync(city_db)
    for tweet in city_db.view('twitter/get_tweets_with_ll'):
        tweets.append({'id': tweet.key, 'text': tweet.value[0], 'lat': tweet.value[1], 'lot': tweet.value[2]})
tweets_df = pd.DataFrame(tweets).drop_duplicates()
tloc = tweets_df[['lot', 'lat']].as_matrix()

place = np.ones(tweets_df.shape[0]) * -1
dist = np.ones(tweets_df.shape[0]) * np.inf
for k, y in yards_df.iterrows():
    yloc = np.ones([tweets_df.shape[0], 2]) * (y.longitude, y.latitude)
    dist_new = haversine_np(tloc[:, 0], tloc[:, 1], yloc[:, 0], yloc[:, 1])
    index = dist > dist_new
    dist[index] = dist_new[index]
    place[index] = np.ones(place[index].shape) * y.objectid

tweets_df['dist']=dist
tweets_df['place']=place
tweets_df=tweets_df[tweets_df.dist<200]
tweets_df['text']=text_pre_process_for_lang(tweets_df)

DetectorFactory.seed=0
lang = []
for k, t in tweets_df.iterrows():
    try:
        lang.append(detect(t.text))
    except langdetect.lang_detect_exception.LangDetectException:
        lang.append('unknow')
tweets_df['lang'] = lang
del lang
analyzer = SentimentIntensityAnalyzer()

sent = []
for k, t in tweets_df.iterrows():
    sent.append(get_sent(t.text, analyzer))
tweets_df['sentiment'] = sent
del sent
tweets_df.place = np.array(tweets_df.place, dtype=np.int16)
final_result = pd.merge(tweets_df, yards_df, left_on='place', right_on='objectid')
sentiment_result=json.loads(final_result.groupby('sportsplayed')['sentiment'].mean()[final_result.groupby('sportsplayed')['sentiment'].count() > 10].to_json())
lang_result = [{st.replace(' ', ''): json.loads(final_result[final_result.sportsplayed == st].groupby('lang').count()['id'].to_json())} for st in list(final_result.groupby('sportsplayed').groups.keys())]

f=open('/Users/xuanzhang/FIT5147SourceCode/web/static/sports_sentiment.json','w+')
f.write(json.dumps(sentiment_result))
f.close()
f=open('/Users/xuanzhang/FIT5147SourceCode/web/static/sports_lang.json','w+')
f.write(json.dumps(lang_result))
f.close()