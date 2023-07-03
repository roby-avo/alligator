import sys 
sys.path.insert(0, '..')
import os
import json
import requests
import pandas as pd
import utils.utils as utils
from tqdm import tqdm
from pymongo import MongoClient


dataset = {
    "Round1_T2D": {
        "tables": "../data/Dataset/Round1_T2D/tables",
        "cea":"../data/Dataset/Round1_T2D/gt/CEA_Round1_gt_WD.csv",
        "cpa":"../data/Dataset/Round1_T2D/gt/CPA_Round1_gt.csv",
        "cta": "../data/Dataset/Round1_T2D/gt/CTA_Round1_gt.csv"
    },
    "HardTableR2-2021": {
        "tables":"../data/Dataset/HardTablesR2/tables/", 
        "cea":"../data/Dataset/HardTablesR2/gt/cea.csv",
        "cpa":"../data/Dataset/HardTablesR2/gt/cpa.csv",
        "cta": "../data/Dataset/HardTablesR2/gt/cta.csv"
    },
    "HardTableR3-2021": {
        "tables":"../data/Dataset/HardTablesR3/tables/", 
        "cea":"../data/Dataset/HardTablesR3/gt/cea.csv",
        "cpa":"../data/Dataset/HardTablesR3/gt/cpa.csv",
        "cta": "../data/Dataset/HardTablesR3/gt/cta.csv"
    },
    "Round3": {
        "tables": "../data/Dataset/Round3_2019/tables/",
        "cea":"../data/Dataset/Round3_2019/gt/CEA_Round3_gt_WD.csv",
        "cpa":"../data/Dataset/Round3_2019/gt/CPA_Round3_gt.csv",
        "cta": "../data/Dataset/Round3_2019/gt/CTA_Round3_gt.csv"
    },
    "2T-2020": {
        "tables":"../data/Dataset/2T_Round4/tables/", 
        "cea":"../data/Dataset/2T_Round4/gt/cea.csv",
        "cpa": None,
        "cta": "../data/Dataset/2T_Round4/gt/cta.csv"
    },
    "Round4": {
        "tables": "../data/Dataset/Round4_2020/tables/",
        "cea":"../data/Dataset/Round4_2020/gt/cea.csv",
        "cpa":"../data/Dataset/Round4_2020/gt/cpa.csv",
        "cta": "../data/Dataset/Round4_2020/gt/cta.csv"
    }
}


headers = {
    'accept': 'application/json',
    'Content-Type': 'application/json',
}

params = (
    ('token', 'selbat_2023'),
    ('kg', 'wikidata')
)


for size in [100]:
    for id_dataset in dataset:
        tables_path, cea_target_path, cpa_target_path, cta_target_path = list(dataset[id_dataset].values())
        id_dataset_new = f"{id_dataset}_{size}_m6"
        buffer = utils.make_buffer_with_cta(f"{id_dataset}_{size}_m4", id_dataset_new, tables_path, cea_target_path, cpa_target_path, cta_target_path, kg_reference="wikidata", candidate_size=size)
        json_data = buffer
        response = requests.post('http://api:5000/dataset/createWithArray', headers=headers, params=params, json=json_data)
        result = response.json()
        