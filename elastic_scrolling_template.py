import sys
import json
import os
import pandas as pd
import yaml
import mysql.connector
from sqlalchemy import create_engine, select
from datetime import datetime, timedelta
import time
from elasticsearch import Elasticsearch

def compute(data_param, q, db_destination_table, engine, db_fields, es_fields):
    data_tracker = data_param
    es_list = []
    json_data=json.loads(q.decode('utf-8'))

    d=dict(json_data)
    # Loops through json output and stores in tabular structure.
    for x in d['hits']['hits']:
        #assigns timestamp to variable, posted to datetime field in table.
        timestamp = x['_source']['@timestamp'].replace("T"," ").replace("Z","")

        temp_list = [timestamp]  # List starts with the timestamp.
        for obj in es_fields:
            path = list(obj.values()) # Path to each elasticsearch value.
            temp_list.append(get_path(path, x)) # Appends each value to the list.
        es_list.append(temp_list) # Temporary list is added to the final es_list.

    # After looping through the results, posts to pandas dataframe, then to SQL table.
    if len(es_list) > 0:
        data_tracker += 1
        print(data_tracker)
        df = pd.DataFrame(es_list,columns=db_fields)
        df.to_sql(name=db_destination_table, con=engine, if_exists = 'append', index=False, chunksize=10000)
        #print(df) # for debugging.

    return data_tracker


def get_path(path_list, resp_object):
    # Path is split into a list.
    for i in path_list:
        res = [a for a in i.split('.')]

    obj = resp_object
    for item in res:
        obj = obj.get(item) # obj becomes each sub-nesting until it gets to the final value.

    return obj

def main(argv):

    try:
        # checks if config file is passed, otherwise defaults to elastic_config.yml.
        if len(argv) != 2:
            config_file = "elastic_config.yml"
        else:
            config_file = argv[1]
        with open(os.path.join(os.getcwd(), config_file), 'r') as ymlfile:
            config = yaml.load(ymlfile)
        
        # Assigns variables from config file.
        db_dialect = config['db']['dialect']
        db_host = config['db']['host']
        db_port = config['db']['port']
        db_user = config['db']['user']
        db_passwd = config['db']['passwd']
        db_db = config['db']['db']
        db_destination_table = config['db']['destination_table']
        db_fields = config['db']['fields']

        es_host = config['es']['host']
        es_event = config['es']['event']
        es_config_table = config['es']['config_table']
        es_index = config['es']['index']
        es_port = config['es']['port']
        es_fields = config['es']['fields']

        #Elasticsearch call using http.
        es = Elasticsearch([es_host],scheme="https", port=es_port, rejectUnauthorized = "false")

        #Create database connection.
        engine = create_engine(db_dialect + '://' + db_user + ':' + db_passwd + '@' + db_host + ':' + str(db_port) + '/' + db_db)
        connection = engine.connect()

        #Select the start date/time, polling interval, and number of periods to query, from SQL config table. For example could be starts at 2020-01-01, interv of 1, period of day.
        s = "SELECT Max(date) as date, Max(interv) as interv, Max(period) as period from " + es_config_table + " WHERE event='" + es_event + "';"
        result = connection.execute(s)
        for row in result:
            myDate=row['date']
            interv=row['interv']
            period=row['period']
        connection.close()

        #Format start_date and end_date for elastic search format.  
        start_date= str(myDate.year)+"-" + str('%02d' % myDate.month)+ "-" + str('%02d' % myDate.day) + "T" + str('%02d' % myDate.hour)+":" + str('%02d' % myDate.minute)+":" + str('%02d' % myDate.second)+"." + str(myDate.microsecond)
        elastic_index = es_index + str(myDate.year) + "." + str('%02d' % myDate.month)+ "." + str('%02d' % myDate.day)
        if period=="hour":
            myDate = myDate + timedelta(hours=interv)
        else:
            myDate = myDate + timedelta(days=interv)
        end_date=str(myDate.year)+"-" + str('%02d' % myDate.month)+ "-" + str('%02d' % myDate.day) + "T" + str('%02d' % myDate.hour)+":" + str('%02d' % myDate.minute)+":" + str('%02d' % myDate.second)+"." + str(myDate.microsecond)

        print("Start Date:",start_date,"End Date:",end_date,"Index:",elastic_index)

        doc = { 
               "query": {
                  "bool":{  
                     "must": [  
                          {  
                           "terms":{  
                              "query_params.event.raw":[  
                                 es_event
                              ]                   
                           }
                        },
                         {  
                           "range":{  
                             "@timestamp":{  
                                 "gte" : start_date,
                                 "lt" :  end_date
                              }
                           }
                        }
                     ],
                    "must_not":[{"terms":{"verb.raw":["OPTIONS","HEAD"]}}] #excludes non-standard http requests.
                  }
              }
        }
       #q = None

        #Attempt 4 requests in case of timeouts.
        requests = 0
        while requests < 4:   
            try:
                #elastic_index is format logstash-ocular-pixel-yyyy.mm.dd
                res = es.search(index=elastic_index, body=doc,request_timeout=1000, scroll = '1m',size = 1000)
                q = json.dumps(res).encode('utf-8')
                sid = res['_scroll_id']
                scroll_size = len(res['hits']['hits'])#res['hits']['total']
                print("scroll_id:",str(sid),"scroll size: " + str(scroll_size))
                data_exists = compute(0, q, db_destination_table, engine, db_fields, es_fields)
                while (scroll_size > 0):
                    res = es.scroll(scroll_id = sid, scroll = '1m')
                    q = json.dumps(res).encode('utf-8')
                    # Update the scroll ID
                    sid = res['_scroll_id']
                    # Get the number of results that we returned in the last scroll
                    scroll_size = len(res['hits']['hits'])
                    print("scroll_id:",str(sid),"scroll size: " + str(scroll_size))
                    data_exists = compute(data_exists, q, db_destination_table, engine, db_fields, es_fields)
                
                requests = 5
            except Exception as e:
                print("Error, elasticsearch attempt", requests + 1, str(e))
                if requests == 3:
                    print("Error Processing Elasticsearch Results:", str(e))
                    raise e
                    sys.exit(1)
                requests += 1

        print(data_exists)
        if data_exists == 0:
            print("Alert: No Data Available from Elasticsearch at ", start_date)
           #sys.exit(1)

        #Updates the date to the next hour.
        s = "UPDATE " + db_db + "." + es_config_table + " SET date='" + end_date + "' WHERE event='" + es_event + "';"
        connection = engine.connect()
        connection.execute(s)
        connection.close()
        print("finished")

        print("elastic query complete")


    except Exception as e:
        print("Unexpected error:", e)
        sys.exit(1)

if __name__ == "__main__":
    main(sys.argv)