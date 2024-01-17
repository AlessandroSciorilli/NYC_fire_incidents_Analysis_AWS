from sodapy import Socrata
import requests
from requests.auth import HTTPBasicAuth
import json
import argparse
import sys
import os

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


DATASET_ID=os.environ["DATASET_ID"]
APP_TOKEN=os.environ["APP_TOKEN"]
ES_HOST=os.environ["ES_HOST"]
ES_USERNAME=os.environ["ES_USERNAME"]
ES_PASSWORD=os.environ["ES_PASSWORD"]
INDEX_NAME=os.environ["INDEX_NAME"]



if __name__ == '__main__': 

    try:
        #Using requests.put(), we are creating an index (db) first.
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
                        "alarm_source_description_tx": {"type": "keyword"},
                        "incident_classification_group": {"type": "keyword"},
                        "dispatch_response_seconds_qy": {"type": "integer"},
                        "incident_response_seconds_qy": {"type": "integer"},
                        "incident_travel_tm_seconds_qy": {"type": "integer"},
                        "engines_assigned_quantity": {"type": "integer"},
                        "ladders_assigned_quantity": {"type": "integer"},
                        "other_units_assigned_quantity": {"type": "integer"},
                        "zipcode": {"type": "keyword"},
                    }
                },
            }
        )
        resp.raise_for_status()
        print(resp.json())
        
 
    except Exception as e:
        print("Index already exists! Skipping")    
    
    client = Socrata("data.cityofnewyork.us", APP_TOKEN, timeout=10000)

    #print(rows)


    #Looping through the number of pages
    for page in range(args.num_pages):
        rows = client.get(DATASET_ID, limit=args.page_size, offset=args.page_size*page )
        es_rows=[]

        for row in rows:
            if row.get("starfire_incident_id") is None or row.get("incident_datetime") is None:
                print("Skipping row due to missing required fields")
                continue
            try:
                # Convert
                es_row = {}
                es_row["starfire_incident_id"] = row["starfire_incident_id"]
                es_row["incident_datetime"] = row["incident_datetime"]
                es_row["incident_borough"] = row["incident_borough"]
                es_row["alarm_source_description_tx"] = row["alarm_source_description_tx"]
                es_row["incident_classification_group"] = row["incident_classification_group"]
                es_row["dispatch_response_seconds_qy"] = int(row["dispatch_response_seconds_qy"])
                es_row["incident_response_seconds_qy"] = int(row["incident_response_seconds_qy"])
                es_row["incident_travel_tm_seconds_qy"] = int(row["incident_travel_tm_seconds_qy"])
                es_row["engines_assigned_quantity"] = int(row["engines_assigned_quantity"])
                es_row["ladders_assigned_quantity"] = int(row["ladders_assigned_quantity"])
                es_row["other_units_assigned_quantity"] = int(row["other_units_assigned_quantity"])
                es_row["zipcode"] = row["zipcode"]
    
                #print(es_row)
            
            except Exception as e:
                print (f"Error!: {e}, skipping row: {row}")
                continue
            
            es_rows.append(es_row)
            print(es_rows)
        
    
        bulk_upload_data = ""
        for line in es_rows:
            print(f'Handling row {line["starfire_incident_id"]}')
            action = '{"index": {"_index": "' + INDEX_NAME + '", "_id": "' + line["starfire_incident_id"] + '"}}'
            data = json.dumps(line)
            bulk_upload_data += f"{action}\n"
            bulk_upload_data += f"{data}\n"
    
        
        try:
            # Upload to Elasticsearch by creating a document
            resp = requests.post(f"{ES_HOST}/_bulk",
                # We upload es_row to Elasticsearch
                        data=bulk_upload_data,auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD), headers = {"Content-Type": "application/x-ndjson"})
            resp.raise_for_status()
            print ('Done')
                
            # If it fails, skip that row and move on.
        except Exception as e:
            print(f"Failed to insert in ES: {e}")
