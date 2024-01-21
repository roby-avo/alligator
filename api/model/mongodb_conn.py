from pymongo import MongoClient

import os

MONGO_ENDPOINT, MONGO_ENDPOINT_PORT = os.environ['MONGO_ENDPOINT'].split(":")
MONGO_ENDPOINT_USERNAME = os.environ['MONGO_INITDB_ROOT_USERNAME']
MONGO_ENDPOINT_PASSWORD = os.environ['MONGO_INITDB_ROOT_PASSWORD']
MONGO_DBNAME = os.environ['MONGO_DBNAME']
mongo_client = MongoClient(
                            MONGO_ENDPOINT, 
                            int(MONGO_ENDPOINT_PORT), 
                            username=MONGO_ENDPOINT_USERNAME, 
                            password=MONGO_ENDPOINT_PASSWORD, 
                            authSource='admin'
                        )

def get_collection(collection_name):
    return mongo_client[MONGO_DBNAME][collection_name]
