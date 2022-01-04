import pymysql
import pandas as pd
import sqlalchemy 

# "us-cdbr-east-04.cleardb.com","b684dfd22ac1cf","17a17075","heroku_0bce2225ad59418"
conn = sqlalchemy.create_engine("mysql+pymysql://root:@localhost/advertising")
dataset = pd.read_sql_table('signage_details', conn)
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
