import os
import sys
import time

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

from process.wrapper.Database import MongoDBWrapper  # MongoDB database wrapper

# Initialize MongoDB wrapper and get collections for different data models
mongoDBWrapper = MongoDBWrapper()
job_c = mongoDBWrapper.get_collection("job")
row_c = mongoDBWrapper.get_collection("row")
dataset_c = mongoDBWrapper.get_collection("dataset")
table_c = mongoDBWrapper.get_collection("table")

UPDATE_FREQUENCY = 5

while True:
    rows_to_consider = row_c.aggregate([
        {"$match": {"state": "READY"}},
        {"$group": {
            "_id": {"datasetName": "$datasetName", "tableName": "$tableName", "idJob":"$idJob", "status": "$status"},  
            "count": {"$sum" : 1}
        }},
        {"$group": {
            "_id": {"datasetName": "$_id.datasetName", "tableName":"$_id.tableName", "idJob":"$_id.idJob"}, 
            "term_tf": {"$push":  { "status": "$_id.status", "count": "$count" }}
        }},
        {"$project": {
            "items": {
                "$map": {
                    "input": "$term_tf",
                    "in": {
                        "k": "$$this.status",
                        "v": "$$this.count"
                    }
                }
            }
        }
        },
        {
        "$project": {
            "status": { "$arrayToObject": "$items" }
        }
        }
    ])
    
    ids_job_to_update = {}
    for row in rows_to_consider:
        dataset_name = row["_id"]["datasetName"]
        table_name = row["_id"]["tableName"] 
        id_job = row["_id"]["idJob"]
        status = row["status"]
        TODO, DOING, DONE = [status.get(key, 0) for key in ["TODO", "DOING", "DONE"]]
        if DOING + DONE == 0:
            continue
        
        status = "DOING"
        if TODO + DOING == 0:
            status = "DONE"
        table_c.update_one(
            {"datasetName": dataset_name, "tableName": table_name}, 
            {"$set": {
                "taskStatus.TODO": TODO,
                "taskStatus.DOING": DOING,
                "taskStatus.DONE": DONE,
                "status": status
        }})      
        if id_job not in ids_job_to_update:
            ids_job_to_update[id_job] = {"TODO": 0, "DOING": 0, "DONE": 0}
        ids_job_to_update[id_job]["TODO"] += TODO
        ids_job_to_update[id_job]["DOING"] += DOING
        ids_job_to_update[id_job]["DONE"] += DONE    
        
       

    for id_job in ids_job_to_update:
        job = job_c.find_one({"_id": id_job})
        elapsed_time = round(time.time() - job["startTime"], 2)
        if job['startTimeComputation'] is None:
            start_time_compuation = time.time()
        else:
            start_time_compuation = job['startTimeComputation']    
        
        elapsed_time_computation = round(time.time() - start_time_compuation, 2)
        
        TODO, DOING, DONE = [ids_job_to_update[id_job].get(key, 0) for key in ["TODO", "DOING", "DONE"]]
        percent = round(DONE/(TODO+DOING+DONE), 2)
        missing_table = TODO + DOING
        estimated_time = round(missing_table * elapsed_time_computation / DONE, 2) if DONE > 0 else None

        tables = table_c.aggregate([
            { "$match": { "idJob": id_job } },  # Filter documents by idJob
            { "$group": {
                "_id": "$status",  # Group by the status field
                "count": { "$sum": 1 }  # Count the number of documents in each group
            }}
        ])
        status = {result["_id"]:result["count"] for result in tables}
        TODO, DOING, DONE = [status.get(key, 0) for key in ["TODO", "DOING", "DONE"]]
        job_c.update_one({"_id": id_job}, {"$set": {
            "status.TODO": TODO, 
            "status.DOING": DOING, 
            "status.DONE": DONE,
            "elapsedTime": elapsed_time,
            "startTimeComputation": start_time_compuation,
            "elapsedTimeComputation": elapsed_time_computation,
            "%": percent,
            "estimatedTime": estimated_time,
            "active": missing_table > 0
        }})
        if missing_table == 0:
            row_c.update_many({"idJob": id_job}, {"$set":{"state": "EXIT"}})
               
   

    datasets = dataset_c.find({})
    for dataset in datasets:
        dataset_name = dataset["datasetName"]
        tables = table_c.aggregate([
            { "$match": { "datasetName": dataset_name } },  # Filter documents by datasetName
            { "$group": {
                "_id": "$status",  # Group by the status field
                "count": { "$sum": 1 }  # Count the number of documents in each group
            }}
        ])
        status = {result["_id"]:result["count"] for result in tables}
        TODO, DOING, DONE = [status.get(key, 0) for key in ["TODO", "DOING", "DONE"]]
        dataset_c.update_one({"datasetName": dataset_name}, {"$set": {
            "status.TODO": TODO, 
            "status.DOING": DOING, 
            "status.DONE": DONE
        }})

    
    time.sleep(UPDATE_FREQUENCY)
