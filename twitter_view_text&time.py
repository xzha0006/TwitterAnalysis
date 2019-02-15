#!/usr/bin/python
import os

import couchdb
import sys
from couchdb.design import ViewDefinition

db_url = 'http://admin:admin@127.0.0.1:5984'
db_list = ['sydney', 'melbourne', 'brisbane', 'perth', 'adelaide', 'gold_coast', 'newcastle', 'canberra', 'wollongong',
           'sunshine_coast', 'hobart', 'geelong', 'townsville', 'cairns', 'toowoomba', 'darwin', 'ballarat']


# Create View
class TweetView():
    def __init__(self):
        self.db_server = couchdb.Server(url=db_url)
        self.db = []
        for i in range(len(db_list)):
            self.create_views(self.db_server[db_list[i] + "_no_repeat"])

    def create_views(self, db_name):
        get_tweets = 'function(doc) { emit(("0000000000000000000"+doc.id).slice(-19), [doc.text, doc.created_at]); }'
        view = couchdb.design.ViewDefinition('twitter', 'get_tweets', get_tweets)
        view.sync(db_name)

TweetView()