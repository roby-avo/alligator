import os
import json
import requests
import pandas as pd
import utils.utils as utils
from tqdm import tqdm
from pymongo import MongoClient
from process.wrapper import mongodb_conn
import numpy as np
import matplotlib.pyplot as plt

dataset = {
    "Round1_T2D": {
        "tables": "./data/Dataset/Round1_T2D/tables",
        "cea":"./data/Dataset/Round1_T2D/gt/CEA_Round1_gt_WD.csv",
        "cpa":"./data/Dataset/Round1_T2D/gt/CPA_Round1_gt.csv",
        "cta": "./data/Dataset/Round1_T2D/gt/CTA_Round1_gt.csv"
    },
    "HardTableR2-2021": {
        "tables":"./data/Dataset/HardTablesR2/tables/", 
        "cea":"./data/Dataset/HardTablesR2/gt/cea.csv",
        "cpa":"./data/Dataset/HardTablesR2/gt/cpa.csv",
        "cta": "./data/Dataset/HardTablesR2/gt/cta.csv"
    },
    "HardTableR3-2021": {
        "tables":"./data/Dataset/HardTablesR3/tables/", 
        "cea":"./data/Dataset/HardTablesR3/gt/cea.csv",
        "cpa":"./data/Dataset/HardTablesR3/gt/cpa.csv",
        "cta": "./data/Dataset/HardTablesR3/gt/cta.csv"
    },
    "Round3": {
        "tables": "./data/Dataset/Round3_2019/tables/",
        "cea":"./data/Dataset/Round3_2019/gt/CEA_Round3_gt_WD.csv",
        "cpa":"./data/Dataset/Round3_2019/gt/CPA_Round3_gt.csv",
        "cta": "./data/Dataset/Round3_2019/gt/CTA_Round3_gt.csv"
    },
    "2T-2020": {
        "tables":"./data/Dataset/2T_Round4/tables/", 
        "cea":"./data/Dataset/2T_Round4/gt/cea.csv",
        "cpa": None,
        "cta": "./data/Dataset/2T_Round4/gt/cta.csv"
    },
    "Round4": {
        "tables": "./data/Dataset/Round4_2020/tables/",
        "cea":"./data/Dataset/Round4_2020/gt/cea.csv",
        "cpa":"./data/Dataset/Round4_2020/gt/cpa.csv",
        "cta": "./data/Dataset/Round4_2020/gt/cta.csv"
    }
}



for size in [10, 100]:
    cea_c = mongodb_conn.get_collection("cea")
    total_wrong = 0
    for id_dataset in dataset:
        tables_path, cea_target_path, cpa_target_path, cta_target_path = list(dataset[id_dataset].values())
        cea_gt = utils.get_cea_ann(cea_target_path)
        ids_dataset = [f"{id_dataset}_{size}_m1"]
        out = {}
        for iteration, name in enumerate(ids_dataset):
            results = cea_c.find({"idDataset": name})
            total = cea_c.count_documents({"idDataset": name})
            for result in tqdm(results, total=total):
                id_table = result["tableName"]
                id_row = result["row"]
                for id_col, item in enumerate(result["winningCandidates"]):
                    key = f"{id_table} {id_row} {id_col}"
                    if key not in cea_gt or cea_gt.get(key) is None:
                        continue
                    if len(item) >= 1:
                        id_entity = item[0]["id"].lower()
                        score = item[0]["score"]
                        cea_score = item[0]["features"]["cea"]
                        outcome = "OK" if id_entity in cea_gt.get(key, []) else "WRONG"
                        if outcome == "WRONG":
                            total_wrong += 1
                        if len(item) > 1:
                            outcome = "WRONG"
                        out[key] = {
                            "datset": id_dataset,
                            "key": key,
                            "gt": cea_gt.get(key),
                            "id": id_entity,
                            "cea_score": cea_score,
                            "score": score,
                            "outcome": outcome
                        }
            buffer = list(out.values())            
            buffer = sorted(buffer, key=lambda x: x["score"], reverse=False)        
            pd.DataFrame(buffer).to_csv(f"data/iterations2/{name}.csv", index=False)     