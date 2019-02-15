#!/usr/bin/python
# -*- coding: utf-8 -*-
import os

import couchdb
import sys
from couchdb.design import ViewDefinition
import json

db_url = 'http://admin:admin@127.0.0.1:5984'
db_name = "aurin_income"
city_list = ['sydney', 'melbourne', 'brisbane', 'perth', 'adelaide', 'gold coast', 'newcastle', 'canberra', 'wollongong',
           'sunshine coast', 'hobart', 'geelong', 'townsville', 'cairns', 'toowoomba', 'darwin', 'ballarat']
city_name = ['sydney', 'melbourne', 'brisbane', 'perth', 'adelaide', 'gold_coast', 'newcastle', 'canberra', 'wollongong',
           'sunshine_coast', 'hobart', 'geelong', 'townsville', 'cairns', 'toowoomba', 'darwin', 'ballarat']

benefit_list = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
total_list = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
benefit_dict = {}

# Create view for aurin data
def create_views():
    server = couchdb.Server(url=db_url)
    db = server[db_name]
    get_benefit = 'function(doc) {emit(doc.area_name, [doc.unem_ben_lg_3_percent_6_13_6_13, doc.unem_ben_lg_1_no_6_13_6_13]);}'
    view = couchdb.design.ViewDefinition('aurin', 'get_benefit', get_benefit)
    view.sync(db)


# Get view
def get_benefit():
    server = couchdb.Server(url=db_url)
    db = server[db_name]
    return db.view('aurin/get_benefit')


# Write JSON File
def save_json(file_name, data):
    with open(file_name, 'w') as f:
        json.dump(data, f)


# AURIN
create_views()
un_emp_benefit = get_benefit()
for city in city_list:
    for line in un_emp_benefit:
        suburb = str(line["key"]).lower()
        if city in suburb and line["value"][0] != 0:
            # print(line["key"], line["value"])
            benefit_list[city_list.index(city)] += line["value"][1]
            total_list[city_list.index(city)] += line["value"][1] / line["value"][0]
for i in range(len(benefit_list)):
    if total_list[i] != 0:
        benefit_list[i] /= total_list[i]
    benefit_dict[city_name[i]] = benefit_list[i]
print(benefit_list)
print(benefit_dict)

# Save in JSON File
save_json("/Users/xuanzhang/FIT5147SourceCode/web/static/sentiment_aurin_benefit.json", benefit_dict)
