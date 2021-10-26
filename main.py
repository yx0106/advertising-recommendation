from flask import Flask, render_template, request, redirect, flash
from experta import *
import pymysql
import pandas as pd
from db_conn import *
import json
import sqlalchemy

myResult = []
signage_name = ''
signage_address = ''
signage_day = ''
signage_popular_time = ''
signage_length = ''
result_array = ''
result_length = ''
data = {}
data['signage_details'] = []

def KnowledgeBase():
    ask_state = myResult[0]
    ask_weekend = myResult[1]
    ask_slot = myResult[2]
    ask_duration = myResult[3]
    class Solution(KnowledgeEngine): # <------ Class initiated for Inference Engine
        @DefFacts()
        def _initial_action(self):
            yield Fact(action="get_Recommend")

        @Rule(Fact(action='get_Recommend'),
            NOT(Fact(state=W())))
        def qa_1(self):
            self.declare(Fact(state=ask_state)) # <------ advertiser requirement is fact 
                                                #   in facts database retrieved from user interface
        
        @Rule(Fact(action='get_Recommend'),
            NOT(Fact(weekend=W())))
        def qa_2(self):
            self.declare(Fact(weekend=ask_weekend))
            
        @Rule(Fact(action='get_Recommend'),
            NOT(Fact(duration=W())))
        def qa_3(self):
            self.declare(Fact(duration=ask_duration))

        @Rule(Fact(action='get_Recommend'),
            NOT(Fact(slot=W())))
        def qa_4(self):
            self.declare(Fact(slot=ask_slot))
                        
        @Rule(AND(Fact(action='get_Recommend'), Fact(state = myResult[0]), Fact(weekend = myResult[1]), Fact(duration = myResult[3]), Fact(slot = myResult[2])))
        def knowledge_1(self):
            if myResult[1] == "yes":
                weekend = 1
            else:
                weekend = 0
            self.declare(Fact(result=myResult[0]), Fact(result2=weekend), Fact(result3=int(myResult[2])), Fact(result4=int(myResult[3])))
        
        @Rule(Fact(action='get_Recommend'),
            Fact(result=MATCH.result), Fact(result2=MATCH.result2), Fact(result3=MATCH.result3), Fact(result4=MATCH.result4))
        def match(self, result, result2, result3, result4):
            getFacts(result)
            getResult(result, result2, result3, result4)
        
    engine = Solution()
    engine.reset()
    engine.run()

    
def getFacts(result):
    return result

def getResult(state, weekend, slot, duration):
    if(weekend == 0):
        results = df2.loc[(df2['state'].str.contains(state))].reset_index(drop=True)
    else:
        results = df1.loc[(df1['state'].str.contains(state))].reset_index(drop=True)
    ids = results['signage_id'][0][:slot]
    result = dataset.loc[(dataset['signage_id'].isin(ids)) & (dataset["weekend"] == weekend)].reset_index(drop=True)
    result = result.groupby(['signage_id']).head(duration)
    global signage_name, signage_length, signage_address, signage_day, signage_popular_time, result_array, result_length
    signage_name = result['name'].unique()
    signage_length = len(signage_name)
    signage_address = result['address'].unique()
    signage_day = result['day'].values
    signage_popular_time = result['popular_time'].values
    signage_day = np.split(signage_day, slot)
    signage_popular_time = np.split(signage_popular_time, slot)
    signage_popular_time = list(map(sum, signage_popular_time))

app = Flask(__name__)
@app.route('/')
def index():
    myResult.clear()
    return render_template('mainpage.html')

@app.route("/start", methods=["GET", "POST"])
def start():
    return render_template('getState.html')


@app.route("/getState", methods=["GET", "POST"])
def getState():
    location = ''
    if request.method == 'POST':
        if request.form.get('Johor') == 'Johor':
            location = 'Johor'
        elif request.form.get('Melaka') == 'Melaka':
            location = 'Melaka'
        elif request.form.get('Negeri Sembilan') == 'Negeri Sembilan':
            location = 'Negeri Sembilan'
        elif request.form.get('Selangor') == 'Selangor':
            location = 'Selangor'
        elif request.form.get('Kuala Lumpur') == 'Kuala Lumpur':
            location = 'Kuala Lumpur'
        elif request.form.get('Putrajaya') == 'Putrajaya':
            location = 'Putrajaya'
        elif request.form.get('Perak') == 'Perak':
            location = 'Perak'
        elif request.form.get('Penang') == 'Penang':
            location = 'Pulau Pinang'
        elif request.form.get('Kedah') == 'Kedah':
            location = 'Kedah'
        elif request.form.get('Kelantan') == 'Kelantan':
            location = 'Kelantan'
        elif request.form.get('Terengganu') == 'Terengganu':
            location = 'Terengganu'
        elif request.form.get('Perlis') == 'Perlis':
            location = 'Perlis'
        elif request.form.get('Pahang') == 'Pahang':
            location = 'Pahang'
        elif request.form.get('Sabah') == 'Sabah':
            location = 'Sabah'
        elif request.form.get('Sarawak') == 'Sarawak':
            location = 'Sarawak'
        elif request.form.get('Labuan') == 'Labuan':
            location = 'Labuan'
        else:
            print("No location can be choose")
        myResult.append(location)
    return render_template("getWeekend.html")

@app.route("/getWeekend", methods=["GET", "POST"])
def getWeekend():
    weekend = ''
    if request.method == 'POST':
        if request.form.get('weekend') == 'weekend':
            weekend = 'yes'
        elif request.form.get('weekday') == 'weekday':
            weekend = 'no'
        else:
            print("No weekend can be choose")
        myResult.append(weekend)
        allLength = df2.loc[(df2['state'].str.contains(myResult[0]))].length.values[0]
        print(allLength)
        weekend = myResult[1]
        print(weekend)
        if allLength == 1:
            myResult.append(allLength)
            return render_template("getDuration.html", weekend = weekend)
        else:
            return render_template("getSlot.html", allLength = allLength)
    return ''

@app.route("/getSlot", methods=["GET", "POST"])
def getSlot():
    if request.method == 'POST':
        slot = request.form.get('Slot')
        myResult.append(slot)
    weekend = myResult[1]
    print(weekend)
    return render_template("getDuration.html", weekend = weekend)

@app.route("/getDuration", methods=["GET", "POST"])
def getDuration():
    duration = ''
    if request.method == 'POST':
        if request.form.get('1') == '1':
            duration = '1'
        elif request.form.get('2') == '2':
            duration = '2'
        elif request.form.get('3') == '3':
            duration = '3'
        elif request.form.get('4') == '4':
            duration = '4'
        elif request.form.get('5') == '5':
            duration = '5'
        else:
            print("No duration can be choose")
        myResult.append(duration)
    KnowledgeBase()
    return render_template("result.html", 
                           signage_name = signage_name, 
                           signage_length = signage_length, 
                           signage_address = signage_address,
                           signage_day = signage_day,
                           signage_popular_time = signage_popular_time)


@app.route("/getRedirect", methods=["GET", "POST"])
def getRedirect():
    return redirect('/')

@app.route("/submitResult", methods=["GET", "POST"])
def submitResult():
    data['signage_details'] = []
    result_list = request.form.getlist('result')
    result_list = list(map(int, result_list))
    result_final = []
    for i in range(signage_length):
        result_array = [signage_name[i], signage_address[i], signage_day[i], signage_popular_time[i]]
        result_final.append(result_array)
    result_length = (len(result_array))
    signage_detail = []
    for i in range(len(result_list)):
        a = result_list[i]
        signage_detail.append(result_final[a])
        data['signage_details'].append({'signage_name': result_final[a][0], 'signage_address': result_final[a][1], 'signage_day': result_final[a][2].tolist(), 'signage_popular_time': result_final[a][3].tolist()})
    advertising_name = request.form.get('advertiser-name')
    advertising_phonenumber = request.form.get('advertiser-phonenumber')
    advertising_email = request.form.get('advertiser-email')
    result = (advertising_name, advertising_email, advertising_phonenumber, str(data))
    conn = pymysql.connect("us-cdbr-east-04.cleardb.com","b684dfd22ac1cf","17a17075","heroku_0bce2225ad59418" )
    cursor = conn.cursor()
    sql = """INSERT INTO advertising_result(
        advertiser_name, advertiser_email, advertiser_phonenumber, advertiser_ads)
        VALUES (%s, %s, %s, %s)"""
    try:
        # Executing the SQL command
        cursor.execute(sql, result)

        # Commit your changes in the database
        conn.commit()
        
    except:
        
        conn.rollback()

    # Closing the connection
    conn.close()
    return redirect('/')



if __name__ == '__main__':
    app.run(debug=True)