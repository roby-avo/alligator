import os
import sys

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

from process.wrapper.mongodb_conn import get_collection

def create_index():
    collections = ['cea', 'cta', 'cpa', 'ceaPrelinking', 'candidateScored']
    for collection in collections:
        c = get_collection(collection)
        c.create_index([('tableName', 1), ('datasetName', 1)])
        c.create_index([('tableName', 1), ('datasetName', 1), ('page', 1)])
        c.create_index([('datasetName', 1)])
        c.create_index([('tableName', 1)])
        
    c = get_collection('row')
    c.create_index([('datasetName', 1)])
    c.create_index([('datasetName', 1), ('tableName', 1)])

    c = get_collection('dataset')
    c.create_index([('datasetName', 1)], unique=True)
    c = get_collection('table')
    c.create_index([('tableName', 1)])
    c.create_index([('datasetName', 1), ('tableName', 1)], unique=True)