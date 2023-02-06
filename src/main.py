from sodapy import Socrata
import requests
from requests.auth import HTTPBasicAuth
import json
import argparse
import sys
import os
import re

# Creates a parser. Parser is the thing where you add your arguments. 
parser = argparse.ArgumentParser(description='311 Requests Data')
# In the parse, we have two arguments to add.
# The first one is a required argument for the program to run. If page_size is not passed in, don’t let the program to run
parser.add_argument('--page_size', type=int, help='how many rows to get per page', required=True)
# The second one is an optional argument for the program to run. It means that with or without it your program should be able to work.
parser.add_argument('--num_pages', type=int, help='how many pages to get in total')
# Take the command line arguments passed in (sys.argv) and pass them through the parser.
# Then you will end up with variables that contains page size and num pages.  
args = parser.parse_args(sys.argv[1:])
print(args)

INDEX_NAME=os.environ["INDEX_NAME"]
DATASET_ID=os.environ["DATASET_ID"]
APP_TOKEN=os.environ["APP_TOKEN"]
ES_HOST=os.environ["ES_HOST"]
ES_USERNAME=os.environ["ES_USERNAME"]
ES_PASSWORD=os.environ["ES_PASSWORD"]

#INDEX_NAME="fire"
#DATASET_ID="8m42-w767"
#APP_TOKEN="TksBNcbpTQxd3rUwbWl53LBPZ"
#ES_HOST ="https://search-project01-greg-teo-ykdzvexlqrvboe3eofb2wyqypm.us-east-2.es.amazonaws.com"
#ES_USERNAME="gmteo"
#ES_PASSWORD="[1]Abcd1234"


if __name__ == '__main__':
    try:
        resp = requests.put(f"{ES_HOST}/{INDEX_NAME}", auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
            json={
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                },
                "mappings": {
                    "properties": {
                        "starfire_incident_id": {"type": "keyword"},
                        "incident_datetime": {"type": "date"},
                        "incident_borough": {"type": "keyword"},
                        "zipcode": {"type": "keyword"},
                        "incident_classification_group": {"type": "keyword"},
                        "incident_response_seconds_qy": {"type": "float"},
                        "engines_assigned_quantity": {"type": "float"},
                    }
                },    
            }
        )
        resp.raise_for_status()
        print(resp.json())
        
    except Exception as e:
        print("Index already exists! Skipping!")
        
    es_rows=[]
    
    if args.num_pages is None:
        client = Socrata("data.cityofnewyork.us", APP_TOKEN, timeout=10000)
        count = [int(s) for s in re.findall(r'-?\d+\.?\d*', str(client.get(DATASET_ID, select='COUNT(*)', where='starfire_incident_id IS NOT NULL AND incident_datetime IS NOT NULL')))]
        #For testing purposes
        #count1 = count[0] - 8690000
        #When ready, change limit = count[0]
        rows = client.get(DATASET_ID, select='*' , where='starfire_incident_id IS NOT NULL AND incident_datetime IS NOT NULL', order = 'incident_datetime', limit = count[0])
        
        for row in rows:
            try:
                # Convert
                es_row = {}
                es_row["starfire_incident_id"] = row["starfire_incident_id"]
                es_row["incident_datetime"] = row["incident_datetime"]
                es_row["incident_borough"] = row["incident_borough"]
                es_row["zipcode"] = row["zipcode"]
                es_row["incident_classification_group"] = row["incident_classification_group"]
                es_row["incident_response_seconds_qy"] = float(row["incident_response_seconds_qy"])
                es_row["engines_assigned_quantity"] = float(row["engines_assigned_quantity"])
           
            except Exception as e:
                #Comment out print command if loading more than 100k
                #print (f"Error!: {e}, skipping row: {row}")
                continue
        
            es_rows.append(es_row)
        
        bulk_upload_data = ""
        for line in es_rows:
            #Comment out print command if loading more than 100k
            #print(f'Handling row {line["starfire_incident_id"]}')
            action = '{"index": {"_index": "' + INDEX_NAME + '", "_type": "_doc", "_id": "' + line["starfire_incident_id"] + '"}}'
            data = json.dumps(line)
            bulk_upload_data += f"{action}\n"
            bulk_upload_data += f"{data}\n"
        #print (bulk_upload_data)
        
        try:
            # Upload to Elasticsearch by creating a document
            resp = requests.post(f"{ES_HOST}/_bulk",
                # We upload es_row to Elasticsearch
                        data=bulk_upload_data,auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD), headers = {"Content-Type": "application/x-ndjson"})
            resp.raise_for_status()
            print ('Done!')
            print ('Total Number of Rows: ',count[0])
            print ('Total Number of Pages: None')
            print('Offset: 0')
              
            # If it fails, skip that row and move on.
        except Exception as e:
            print(f"Failed to insert in ES: {e}")
            
        #print(es_rows)
        es_rows.clear()
    
    else:
        for num_pages in range(args.num_pages):
            offset = num_pages * args.page_size
            client = Socrata("data.cityofnewyork.us", APP_TOKEN, timeout=10000)
            rows = client.get(DATASET_ID, limit=args.page_size, where='starfire_incident_id IS NOT NULL AND incident_datetime IS NOT NULL', offset = {offset}, order = 'incident_datetime')
     
            for row in rows:
                try:
                    # Convert
                    es_row = {}
                    es_row["starfire_incident_id"] = row["starfire_incident_id"]
                    es_row["incident_datetime"] = row["incident_datetime"]
                    es_row["incident_borough"] = row["incident_borough"]
                    es_row["zipcode"] = row["zipcode"]
                    es_row["incident_classification_group"] = row["incident_classification_group"]
                    es_row["incident_response_seconds_qy"] = float(row["incident_response_seconds_qy"])
                    es_row["engines_assigned_quantity"] = float(row["engines_assigned_quantity"])
               
                except Exception as e:
                    #Comment out print command if loading more than 100k
                    #print (f"Error!: {e}, skipping row: {row}")
                    continue
            
                es_rows.append(es_row)
            #print(es_rows)
            #print('offset: ', offset)
            #print('num pages: ', num_pages)
            
            bulk_upload_data = ""
            for line in es_rows:
                #Comment out print command if loading more than 100k
                #print(f'Handling row {line["starfire_incident_id"]}')
                action = '{"index": {"_index": "' + INDEX_NAME + '", "_type": "_doc", "_id": "' + line["starfire_incident_id"] + '"}}'
                data = json.dumps(line)
                bulk_upload_data += f"{action}\n"
                bulk_upload_data += f"{data}\n"
            #print (bulk_upload_data)
            
            try:
                # Upload to Elasticsearch by creating a document
                resp = requests.post(f"{ES_HOST}/_bulk",
                    # We upload es_row to Elasticsearch
                            data=bulk_upload_data,auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD), headers = {"Content-Type": "application/x-ndjson"})
                resp.raise_for_status()
                    
                # If it fails, skip that row and move on.
            except Exception as e:
                print(f"Failed to insert in ES: {e}")
            
            #print(es_rows)
            es_rows.clear()
            
        print ('Done!')
        print ('Total Number of Rows: ',args.num_pages * args.page_size)
        print ('Total Number of Pages: ', args.num_pages)
        print('Offset for page',num_pages+1,': ', offset)        
