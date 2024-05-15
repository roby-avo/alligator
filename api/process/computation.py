import asyncio
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
from phases.decision import Decision
from wrapper.lamAPI import LamAPI
from wrapper.Database import MongoDBWrapper  # MongoDB database wrapper



async def main():
    start = time.time()

    pn_neural_path = "./process/ml_models/Linker_PN_100.h5"
    rn_neural_path = "./process/ml_models/Linker_RN_100.h5"

    pn_model = load_model(pn_neural_path)    
    rn_model = load_model(rn_neural_path)    


    REDIS_ENDPOINT = os.environ["REDIS_ENDPOINT"]
    REDIS_JOB_DB = int(os.environ["REDIS_JOB_DB"])
    LAMAPI_HOST = os.environ["LAMAPI_ENDPOINT"]
    LAMAPI_TOKEN = os.environ["LAMAPI_TOKEN"]


    job_active = redis.Redis(host=REDIS_ENDPOINT, db=REDIS_JOB_DB)

    # Initialize MongoDB wrapper and get collections for different data models
    mongoDBWrapper = MongoDBWrapper()
    log_c = mongoDBWrapper.get_collection("log")
    row_c = mongoDBWrapper.get_collection("row")
    candidate_scored_c = mongoDBWrapper.get_collection("candidateScored")
    cea_c = mongoDBWrapper.get_collection("cea")
    cpa_c = mongoDBWrapper.get_collection("cpa")
    cta_c = mongoDBWrapper.get_collection("cta")
    cea_prelinking_c = mongoDBWrapper.get_collection("ceaPrelinking")
   
    data = row_c.find_one_and_update({"status": "TODO"}, {"$set": {"status": "DOING"}})

    if data is None:
        print("No data to process", flush=True)
        job_active.set("STOP", "")
        sys.exit(0)

    rows_data = data["rows"]
    kg_reference = data["kgReference"]
    limit = data["candidateSize"]
    column_metadata = data["column"]
    target = data["target"]
    _id = data["_id"]
    dataset_name = data["datasetName"]
    table_name = data["tableName"]
    page = data["page"]
    header = data["header"]

    lamAPI = LamAPI(LAMAPI_HOST, LAMAPI_TOKEN, mongoDBWrapper, kg=kg_reference)

    obj_row_update = {"status": "DONE", "time": None}
    dp = DataPreparation(header, rows_data, lamAPI)
    
    try:
        column_metadata, target = await dp.compute_datatype(column_metadata, target)
        if target["SUBJ"] is not None:
            column_metadata[str(target["SUBJ"])] = "SUBJ"
        obj_row_update["column"] = column_metadata
        obj_row_update["metadata"] = {
            "column": [{"idColumn": int(id_col), "tag": column_metadata[id_col]} for id_col in column_metadata]
        }
        obj_row_update["target"] = target
            
        metadata = {
            "datasetName": dataset_name,
            "tableName": table_name,
            "kgReference": kg_reference,
            "page": page
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
        await l.generate_candidates()
        rows = l.get_rows()
        features = await FeauturesExtraction(rows, lamAPI).compute_feautures()
        Prediction(rows, features, pn_model).compute_prediction("rho")
        cea_preliking_data = utils.get_cea_pre_linking_data(metadata, rows)
        revision = FeaturesExtractionRevision(rows)
        features = revision.compute_features()
        Prediction(rows, features, rn_model).compute_prediction("rho'")
        storage = Decision(metadata, cea_preliking_data, rows, revision._cta, revision._cpa_pair, collections)
        storage.store_data()
        end = time.time()
        execution_time = round(end - start, 2)
        obj_row_update["time"] = execution_time
        row_c.update_one({"_id": _id}, {"$set": obj_row_update})
        print("End", flush=True)
    except Exception as e:
        log_c.insert_one({
            "datasetName": dataset_name, 
            "tableName": table_name, 
            "error": str(e), 
            "stackTrace": traceback.format_exc()
        })


# Run the asyncio event loop
asyncio.run(main())
