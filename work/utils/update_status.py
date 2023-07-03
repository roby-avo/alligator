import os
import sys
import time

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)

from process.wrapper.mongodb_conn import get_collection

job_c = get_collection("job")
row_c = get_collection("row")
dataset_c = get_collection("dataset")
table_c = get_collection("table")

UPDATE_FREQUENCY = 5

while True:
    job_in_doing = row_c.aggregate([
        {"$match": {"state": "READY"}},
        {"$group": {
            "_id": {"datasetName": "$datasetName", "status": "$status"},  
            "count": {"$sum" : 1}
        }},
        {"$group": {
            "_id": "$_id.datasetName",  
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

    dataset_in_doing = row_c.aggregate([
        {"$match": {"state": "READY"}},
        {"$group": {
            "_id": {"datasetName": "$datasetName", "status": "$status"},  
            "count": {"$sum" : 1}
        }},
        {"$group": {
            "_id": "$_id.datasetName",  
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

    table_in_doing = row_c.aggregate([
        {"$match": {"state": "READY"}},
        {"$group": {
            "_id": {"datasetName": "$datasetName", "tableName": "$tableName", "status": "$status"},  
            "count": {"$sum" : 1}
        }},
        {"$group": {
            "_id": {"datasetName": "$_id.datasetName", "tableName":"$_id.tableName"}, 
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

   
    dataset_in_doing = {}
    for item in table_in_doing:
        dataset_name = item["_id"]["datasetName"]
        table_name = item["_id"]["tableName"]
        if dataset_name not in dataset_in_doing:
            dataset_in_doing[dataset_name] = {"TODO": 0, "DOING":0, "DONE": 0}
        if len(item["status"]) > 1:
            dataset_in_doing[dataset_name]["DOING"] += 1
        else:
            dataset_in_doing[dataset_name][list(item["status"].keys())[0]] += 1       
        result = table_c.find_one({"tableName": table_name})
        new_status = {status:item["status"].get(status, 0)+result["statusCopy"].get(status, 0) for status in result["statusCopy"]}
        temp = {}
        if new_status["TODO"] + new_status["DOING"] == 0:
            temp = { 
                "statusCopy.TODO": new_status["TODO"], 
                "statusCopy.DOING": new_status["DOING"], 
                "statusCopy.DONE": new_status["DONE"],
                "process": "DONE"
            }
        temp = dict({"status": new_status}, **temp)
        table_c.update_one({"datasetName": dataset_name, "tableName": table_name}, {"$set": temp})   

    
    for dataset in dataset_in_doing:
        result = dataset_c.find_one({"datasetName": dataset_name})
        if result is not None:
            new_status = {status:dataset_in_doing[dataset_name].get(status, 0)+result["statusCopy"].get(status, 0) for status in result["statusCopy"]}
            temp = {}
            if new_status["TODO"] + new_status["DOING"] == 0:
                temp = { 
                    "statusCopy.TODO": new_status["TODO"], 
                    "statusCopy.DOING": new_status["DOING"], 
                    "statusCopy.DONE": new_status["DONE"],
                    "process": "DONE"
                }
            temp = dict({"status": new_status}, **temp)
            dataset_c.update_one({"datasetName": dataset_name}, {"$set": temp})


    for item in job_in_doing:
        name = item["_id"]
        print("datasetName:", dataset_name, flush=True)
        job = job_c.find_one({"name": name, "active": True})
        
        if job is None:
            job_c.insert_one({
                "name": name,
                "status": {
                    "TODO":0, 
                    "DOING": 0, 
                    "DONE": 0
                },
                "startTime": time.time(),
                "startTimeComputation": None,
                "elapsedTime": 0,
                "elapsedTimeComputation": 0,
                "%": 0,
                "estimatedTime": None,
                "active": True
            })
        else:
            _id = job["_id"]
            TODO, DOING, DONE = [item["status"].get(status, 0) for status in job["status"]]
            elapsed_time = round(time.time() - job["startTime"], 2)
            start_time_computation = 0
            elapsed_time_computation = 0
            update_obj = {}
            if DOING > 0 and job['startTimeComputation'] is None:
                start_time_computation = time.time()
                if start_time_computation - job["startTime"] <= (UPDATE_FREQUENCY+2):
                    start_time_computation =  job["startTime"]
                update_obj["startTimeComputation"] = start_time_computation
            elif job['startTimeComputation'] is not None:
                elapsed_time_computation = round(time.time() - job['startTimeComputation'], 2)   
            percent = round(DONE/(TODO+DOING+DONE), 2)
            missing_blocks = TODO + DOING
            estimated_time = round(missing_blocks * elapsed_time_computation / DONE, 2) if DONE > 0 else None
            temp = {
                "status.TODO": TODO, 
                "status.DOING": DOING, 
                "status.DONE": DONE,
                "elapsedTime": elapsed_time,
                "elapsedTimeComputation": elapsed_time_computation,
                "%": percent,
                "estimatedTime": estimated_time
            }
            update_obj = dict(update_obj, **temp)
            job_c.update_one({"_id": _id}, {"$set": update_obj})
            if (TODO + DOING) == 0:
                row_c.update_many({"datasetName": name, "status": "DONE"}, {"$set":{"state": "EXIT"}})
                job_c.update_one({"_id": _id}, {"$set": { "active": False }})
                       
    #print("update status..", flush=True)
    time.sleep(UPDATE_FREQUENCY)