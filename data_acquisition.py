import requests
import io
import pandas as pd
import json
from sqlalchemy import create_engine
import pymysql

API = "https://fuyoh-ads.com/api/pricing_decision/sample"
response = requests.get(API)
print(response)
json = response.json()
signage = json['data']['signage']
popularTime = json['data']['popular_time']

dfSignage = pd.read_csv(io.StringIO(signage), lineterminator='\n', sep='\t')
dfPT = pd.read_csv(io.StringIO(popularTime), lineterminator='\n', sep='\t')

import json

dfPT.fillna(method ='ffill',inplace = True)
dfPT = dfPT.dropna()
signage_id = []
day = []
hour = []
popular_times = []
total_hours = 24

for record in dfPT.signage_id:
    for i in range(168):
        signage_id.append(record)

for record in dfPT.popular_time:
    pop_times = json.loads(record)
    for pop_time in pop_times:
        for j in range (0,total_hours) :
            day.append(pop_time['name'])
            hour.append(j)
            popular_times.append(pop_time['data'][j])
                
dfPop = pd.DataFrame()
dfPop.insert(0, "signage_id", signage_id)
dfPop.insert(1, "day", day)
dfPop.insert(2, "hour", hour)
dfPop.insert(3, "popular_time", popular_times)

stateList = []
for record in dfSignage.address:
    address = record.rsplit(',')
    stateList.append(address[-1])

dfSignage.insert(6, "state", stateList)

ratingAvgList = []
ratingSum = 0
ratingCount = 0
for record in dfSignage.nearby: 
    nearbys = json.loads(record)
    for nearby in nearbys: 
        if 'rating' not in nearby: 
            continue
        ratingSum += nearby['rating']
        ratingCount += 1
    ratingAvg = ratingSum / ratingCount
    ratingAvgList.append(ratingAvg)

dfSignage.insert(5, "nearbyRatingAvg", ratingAvgList)
dfSignage = dfSignage.drop("nearby", axis=1)
dfSignage['state'] = dfSignage['state'].replace(['Malacca'],'Melaka')
dfSignage['state'] = dfSignage['state'].replace(['Federal Territory of Kuala Lumpur'],'Wilayah Persekutuan Kuala Lumpur')

combined = dfPop.merge(dfSignage,how ='left', left_on = ['signage_id'], right_on = ['signage_id'])
combined.loc[(combined['state'].str.contains("Johor|Kedah|Kelantan|Terengganu")) & (combined["day"].str.contains("Friday|Saturday")), "weekend"] = 1
combined.loc[~(combined['state'].str.contains("Johor|Kedah|Kelantan|Terengganu")) & (combined["day"].str.contains("Sunday|Saturday")), "weekend"] = 1
combined['weekend'] = combined['weekend'].fillna(0)

dataset = combined.groupby(['signage_id', 'day', 'name', 'address', 'state']).sum()
g = dataset['popular_time'].groupby(['signage_id'], group_keys=False)
res = g.apply(lambda x: x.sort_values(ascending=False)).reset_index()
combined2 = res.merge(dataset,how ='left', left_on = ['signage_id', 'day', 'name', 'address', 'state', 'popular_time'], right_on = ['signage_id', 'day', 'name', 'address', 'state', 'popular_time'])

combined2['weekend'] = combined2['weekend'].replace([24],1)
combined2 = combined2.drop(["group_id", "longitude", "latitude", 'nearbyRatingAvg', 'hour'], axis=1)
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}".format(user="root", pw="", db="advertising"))
combined2.to_sql('signage_details', con = engine, if_exists = 'replace', chunksize = 1000)

combined2.to_csv('dataset.csv')