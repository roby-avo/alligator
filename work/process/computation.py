import os
import sys
import time
import traceback

import redis
from keras.models import load_model

import utils.utils as utils
from phases.data_preparation import DataPreparation
from phases.featuresExtractionRevision import FeaturesExtractionRevision
from phases.feauturesExtraction import FeauturesExtraction
from phases.lookup import Lookup
from phases.prediction import Prediction
from phases.storage import Storage
from wrapper.lamAPI import LamAPI
from wrapper.mongodb_conn import get_collection

start = time.time()

neural2_path = "./process/ml_models/neural_network2.h5"
model = load_model(neural2_path)
REDIS_ENDPOINT = os.environ["REDIS_ENDPOINT"]
REDIS_JOB_DB = int(os.environ["REDIS_JOB_DB"])
LAMAPI_HOST, LAMAPI_PORT = os.environ["LAMAPI_ENDPOINT"].split(":")
LAMAPI_TOKEN = os.environ["LAMAPI_TOKEN"]


job_active = redis.Redis(host=REDIS_ENDPOINT, db=REDIS_JOB_DB)

row_c = get_collection('row')
log_c = get_collection('log')
cea_prelinking_c = get_collection('ceaPrelinking')
header_cea_c = get_collection('ceaHeader')
cea_c = get_collection('cea')
cpa_c = get_collection('cpa')
cta_c = get_collection('cta')
header_candidate_scored_c = get_collection('headerCandidateScored')
candidate_scored_c = get_collection('candidateScored')

data = row_c.find_one_and_update({"status": "TODO"}, {"$set": {"status": "DOING"}})


if data is None:
    job_active.set("STOP", "")
    sys.exit(0)

header = data.get("header", [])
rows_data = data["rows"]
kg_reference = data["kgReference"]
limit = data["candidateSize"]
column_metadata = data["column"]
target = data["target"]
_id = data["_id"]
dataset_name = data["datasetName"]
table_name = data["tableName"]


lamAPI = LamAPI(LAMAPI_HOST, LAMAPI_PORT, LAMAPI_TOKEN, kg=kg_reference)

obj_row_update = {"status": "DONE", "time": None}
dp = DataPreparation(rows_data, lamAPI)

try:
    if len(column_metadata) == 0:
        column_metadata, target = dp.compute_datatype()
        column_metadata[str(target["SUBJ"])] = "SUBJ"
        obj_row_update["column"] = column_metadata
        obj_row_update["metadata"] = {
            "column": [{"idColumn": int(id_col), "tag": column_metadata[id_col]} for id_col in column_metadata]
        }
        obj_row_update["target"] = target
        
    metadata = {
        "datasetName": dataset_name,
        "tableName": table_name,
        "kgReference": kg_reference
    }

    collections = {
        "ceaPrelinking": cea_prelinking_c,
        "cea": cea_c,
        "cta": cta_c,
        "cpa": cpa_c,
        "candidateScored": candidate_scored_c
    }
    dp.rows_normalization()
    l = Lookup(data, lamAPI, target, log_c, kg_reference, limit)
    rows = l.get_rows()
    features = FeauturesExtraction(rows, lamAPI).compute_feautures()
    Prediction(rows, features, model).compute_prediction("cea")
    cea_preliking_data = utils.get_cea_pre_linking_data(metadata, rows)
    revision = FeaturesExtractionRevision(rows)
    features = revision.compute_features()
    Prediction(rows, features, model).compute_prediction("score")
    storage = Storage(metadata, cea_preliking_data, rows, revision._cta, revision._cpa_pair, collections)
    storage.store_data()
    end = time.time()
    execution_time = round(end - start, 2)
    obj_row_update["time"] = execution_time
    row_c.update_one({"_id": _id}, {"$set": obj_row_update})
except Exception as e:
     log_c.insert_one({
        "datasetName": dataset_name, 
        "tableName": table_name, 
        "error": str(e), 
        "stackTrace": traceback.format_exc()
    })
