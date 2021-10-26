import pymysql
import pandas as pd

#connection = pymysql.connect(host='us-cdbr-east-04.cleardb.com',
                             #user='b684dfd22ac1cf',
                             #password='17a17075',
                            #  db='heroku_0bce2225ad59418')
# SQL_Query = pd.read_sql_query(
        # '''select *
        # from signage_details''', connection)

# dataset = pd.DataFrame(SQL_Query, columns=['signage_id', 'day', 'name', 'address', 'state', 'popular_time', 'hour', 'weekend'])
dataset = pd.read_csv('dataset.csv')    
import numpy as np
allState = dataset['state'].unique()
stateList = dataset['state'].unique()
for x in range(len(allState)):
    allState[x] = dataset.loc[(dataset['state'].str.contains(allState[x])) & (dataset["weekend"] == 1)]
    allState[x] = allState[x].sort_values(by=['popular_time'], ascending=False)
    allState[x] = allState[x]['signage_id'].unique().tolist()
stateList = stateList.reshape((len(stateList),1))   
allState = allState.reshape((len(allState),1))
State_list_no_weekend = np.concatenate((stateList,allState), axis = 1)

df1 = pd.DataFrame(State_list_no_weekend, 
                  columns=['state', 
                      'signage_id'])

df1['length'] = df1['signage_id'].apply(len)

import numpy as np
allState = dataset['state'].unique()
stateList = dataset['state'].unique()
for x in range(len(allState)):
    allState[x] = dataset.loc[(dataset['state'].str.contains(allState[x])) & (dataset["weekend"] == 0)]
    allState[x] = allState[x].sort_values(by=['popular_time'], ascending=False)
    allState[x] = allState[x]['signage_id'].unique().tolist()
stateList = stateList.reshape((len(stateList),1))   
allState = allState.reshape((len(allState),1))
State_list_no_weekend = np.concatenate((stateList,allState), axis = 1)

df2 = pd.DataFrame(State_list_no_weekend, 
                  columns=['state', 
                      'signage_id'])

df2['length'] = df2['signage_id'].apply(len)

