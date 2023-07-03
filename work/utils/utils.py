import json
import os
import statistics
import sys

import pandas as pd
from dateutil.parser import parse
from pymongo import MongoClient
from tqdm import tqdm

currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)


sys.path.append(parentdir)
DBNAME = os.environ["MONGO_DBNAME"]
MONGO_ENDPOINT, MONGO_ENDPOINT_PORT = os.environ["MONGO_ENDPOINT"].split(":")
MONGO_ENDPOINT_USERNAME = os.environ["MONGO_INITDB_ROOT_USERNAME"]
MONGO_ENDPOINT_PASSWORD = os.environ["MONGO_INITDB_ROOT_PASSWORD"]
client = MongoClient(MONGO_ENDPOINT, int(MONGO_ENDPOINT_PORT), username=MONGO_ENDPOINT_USERNAME, password=MONGO_ENDPOINT_PASSWORD)
cea_c = client[DBNAME].cea
cea_c = client[DBNAME].ceaPrelinking
#cea_c = client[DBNAME].ceaInit
candidate_scored_c = client[DBNAME].candidateScored
cpa_c = client[DBNAME].cpa
cta_c = client[DBNAME].cta


def get_candidate_scored(dataset_name, table_name, id_row):
    return candidate_scored_c.find_one({"datasetName": dataset_name, "tableName": table_name, "row": id_row})

def get_cea_ann(cea_gt_path):
    gt_files = pd.read_csv(cea_gt_path)
    target = {}
    for row in tqdm(gt_files.itertuples(), total=len(gt_files)):
        id_table, id_row, id_col, gt = (row[i] for i in range(1, 5))
        key = f"{id_table} {id_row} {id_col}"
        id_table, id_row, id_col, gt = (row[i] for i in range(1, 5))
        entities = gt.split(' ')
        idx = entities[0].rfind('/')
        entities = [a[idx+1:].lower() if idx != -1 else a[31:] for a in entities]
        target[key] = entities
    return target  

def get_cta_ann(cta_gt_path):
    gt_files = pd.read_csv(cta_gt_path)
    target = {}
    for row in tqdm(gt_files.itertuples(), total=len(gt_files)):
        id_table, id_col, gt = (row[i] for i in range(1, 4))
        key = f"{id_table} {id_col}"
        target[key] = gt[31:]
    return target 

def get_my_annotation_filtered(my_annotations, target_file):
    for key in list(my_annotations.keys()):
        if key not in target_file:
            del my_annotations[key]

            
def get_cea_target(cea_gt_path):
    gt_files = pd.read_csv(cea_gt_path)
    target = {}
    for row in tqdm(gt_files.itertuples(), total=len(gt_files)):
        id_table, id_row, id_col = (row[i] for i in range(1, 4))
        key = f"{id_table} {id_row} {id_col}"
        target[key] = True
    return target  

def get_cpa_target(cpa_gt_path):
    gt_files = pd.read_csv(cpa_gt_path)
    target = {}
    for row in tqdm(gt_files.itertuples(), total=len(gt_files)):
        id_table, id_subj, id_obj = (row[i] for i in range(1, 4))
        key = f"{id_table} {id_subj} {id_obj}"
        target[key] = True
    return target    


def get_cta_target(cta_gt_path):
    gt_files = pd.read_csv(cta_gt_path)
    target = {}
    for row in tqdm(gt_files.itertuples(), total=len(gt_files)):
        id_table, id_col = (row[i] for i in range(1, 3))
        key = f"{id_table} {id_col}"
        target[key] = True
    return target    


def get_my_cea_annotation(id_dataset, resolve_disambiguation=True):
    results = cea_c.find({"datasetName": id_dataset})
    total = cea_c.count_documents({"datasetName": id_dataset})
    out = {}
    for result in tqdm(results, total=total):
        id_table = result["tableName"]
        id_row = result["row"]
        for id_col, item in enumerate(result["winningCandidates"]):
            key = f"{id_table} {id_row} {id_col}"
            #if key == "":
            #    print(item)
            if len(item) == 1:
                #if item[0]["score"] >= 0.80:
                out[key] = item[0]["id"].lower() 
            elif len(item) > 1 and resolve_disambiguation:
                out[key] = item[0]["id"].lower()             
    return out 


def get_my_cea_annotation_NIL(id_dataset, NIL_entities, buffer):
    results = cea_c.find({"datasetName": id_dataset})
    total = cea_c.count_documents({"datasetName": id_dataset})
    history = set()
    for result in tqdm(results, total=total):
        id_table = result["tableName"]
        id_row = result["row"]
        for id_col, item in enumerate(result["winningCandidates"]):
            key = f"{id_table} {id_row} {id_col}"
            if len(item) > 0:
                if key in NIL_entities:
                    if key not in history:
                        buffer[id_dataset].append(item[0]["score"])
                        history.add(key)



def get_my_cea_annotation_only_match_false(id_dataset, ids=[]):
    ids = [id_dataset] + ids
    out = {}
    delta = 0
    for id_dataset in ids:
        results = cea_c.find({"datasetName": id_dataset})
        total = cea_c.count_documents({"datasetName": id_dataset})
        for result in tqdm(results, total=total):
            id_table = result["tableName"]
            id_row = result["row"]
            for id_col, item in enumerate(result["winningCandidates"]):
                key = f"{id_table} {id_row} {id_col}"
                if len(item) > 1:
                    out[key] = item[0]["id"].lower()
                    delta += (item[0]["score"] - item[1]["score"])
    delta = round(delta/len(out), 3)
    return out, delta


def get_my_cea_annotation_scored(id_dataset):
    results = candidate_scored_c.find({"datasetName": id_dataset})
    total = candidate_scored_c.count_documents({"datasetName": id_dataset})
    out = {}
    for result in tqdm(results, total=total):
        id_table = result["tableName"]
        id_row = result["row"]
        for id_col, items in enumerate(result["candidates"]):
            key = f"{id_table} {id_row} {id_col}"
            out[key] = set()
            for item in items:
                #item = item[1]
                out[key].add(item["id"].lower())
                
    return out 


def get_my_cta_annotation(id_dataset):
    results, _ = get_cta_types_table_to_column(id_dataset)
    out = {}
    for id_table in tqdm(results):
        for id_col in results[id_table]:
            key = f"{id_table} {id_col}"
            winning_types = sorted(results[id_table][id_col].items(), key=lambda x: x[1], reverse=True)
            if len(winning_types) > 0:
                winning_type = winning_types[0][0]
                if results[id_table][id_col].get("Q5", 0) > 0.80:
                    winning_type = "Q5"
                out[key] = winning_type
    return out 

  
def get_my_cpa_annotation(id_dataset):
    results = cpa_c.find({"datasetName": id_dataset})
    total = cpa_c.count_documents({"datasetName": id_dataset})
    out = {}
    for result in tqdm(results, total=total):
        id_table = result["tableName"]
        for id_col in result["cpa"]:
            key = f"{id_table} 0 {id_col}"
            out[key] = result["cpa"][id_col]
    return out 



def cea_eval(id_dataset, cea_gt_path, resolve_disambiguation=True):
    my_annotations = get_my_cea_annotation(id_dataset, resolve_disambiguation)
    a = 0
    gt_ann = get_cea_ann(cea_gt_path)
    total_my_ann = 0    
    errors = []
    for key in my_annotations:
        if key not in gt_ann:
            continue
        if my_annotations.get(key) in gt_ann[key]:
            a += 1
        else:
            errors.append(key)
        total_my_ann += 1   
    p = (a/total_my_ann) if total_my_ann > 0 else 0
    r = (a/len(gt_ann))
    f1 = (2*p*r)/(p+r) if total_my_ann > 0 else 0
    return {"P": p, "R": r, "F1": f1}, my_annotations,  errors


def cea_eval_missing(id_dataset, cea_gt_path):
    my_annotations = get_my_cea_annotation_scored(id_dataset)
    a = 0
    gt_ann = get_cea_ann(cea_gt_path)
    missings = []
    for key in gt_ann:
        found = False
        for ann in gt_ann[key]:
            if ann in my_annotations.get(key, {}):
                found = True
                a += 1
                break
        if not found:
            missings.append([key, [_id.upper() for _id in gt_ann[key]]])            
    return round(a / len(gt_ann), 2), missings


def cea_eval_my_ann(my_annotations, gt_ann):
    a = 0
    total_my_ann = 0    
    for key in my_annotations:
        if key not in gt_ann:
            continue
        if my_annotations.get(key) in gt_ann[key]:
            a += 1
        total_my_ann += 1   
    p = (a/total_my_ann) if total_my_ann > 0 else 0
    r = (a/len(gt_ann))
    f1 = (2*p*r)/(p+r) if total_my_ann > 0 else 0
    return {"P": p, "R": r, "F1": f1}


def cpa_eval(id_dataset, cpa_gt_path):
    my_annotations = get_my_cea_annotation(id_dataset)
    gt_files = pd.read_csv(cpa_gt_path)
    a = 0
    for row in tqdm(gt_files.itertuples(), total=len(gt_files)):
        id_table, id_subj, id_obj, gt = (row[i] for i in range(1, 5))
        key = f"{id_table} {id_obj}"
        gt_ann = [a[31:]  for a in gt.split(' ')]
        gt_ann[key] = gt_ann
        
    total_my_ann = 0    
    for key in my_annotations:
        if key not in gt_ann:
            continue
        if my_annotations.get(key) in gt_ann[key]:
            a += 1
        total_my_ann += 1   
    p = (a/total_my_ann) if total_my_ann > 0 else 0
    r = (a/len(gt_files))
    f1 = (2*p*r)/(p+r) if total_my_ann > 0 else 0
    return {"P": p, "R": r, "F1": f1}


def cta_eval(id_dataset, cta_gt_path, gt_ancestor_path, gt_descendent_path):
    gt_ancestor = json.load(open(gt_ancestor_path))
    gt_descendent = json.load(open(gt_descendent_path))
    my_annotations = get_my_cta_annotation(id_dataset)
    gt_files = pd.read_csv(cta_gt_path)
    total_score = 0
    total_my_ann = 0
    for row in tqdm(gt_files.itertuples(), total=len(gt_files)):
        id_table, id_col, gt = (row[i] for i in range(1, 4))
        key = f"{id_table} {id_col}"
        annotation = my_annotations.get(key)
        if annotation is None:
            continue
        if not annotation.startswith('http://www.wikidata.org/entity/'):
            annotation = 'http://www.wikidata.org/entity/' + annotation    
        total_my_ann += 1    
        max_score = 0
        for gt_type in gt.split(' '):
            ancestor = gt_ancestor[gt_type]
            ancestor_keys = [k.lower() for k in ancestor]
            descendent = gt_descendent[gt_type]
            descendent_keys = [k.lower() for k in descendent]
            if annotation.lower() == gt_type.lower():
                score = 1.0
            elif annotation.lower() in ancestor_keys:
                depth = int(ancestor[annotation])
                if depth <= 5:
                    score = pow(0.8, depth)
                else:
                    score = 0
            elif annotation.lower() in descendent_keys:
                depth = int(descendent[annotation])
                if depth <= 3:
                    score = pow(0.7, depth)
                else:
                    score = 0
            else:
                score = 0
            if score > max_score:
                max_score = score
        total_score += max_score
    precision = total_score / total_my_ann if total_my_ann > 0 else 0
    recall = total_score / len(gt_files)
    f1 = (2 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0    
    return {"P": precision, "R": recall, "F1": f1}


def get_cpa_top1(ids_dataset):
    out = {}
    results = cpa_c.aggregate([{ "$match": { "idDataset": {"$in": ids_dataset}} },
    {
        "$project": {
            "idDataset": 1,
            "tableName":1,
            "vectors": {
                "$objectToArray": "$winningCandidates"
            }
        },
    },
    { "$unwind": "$vectors" },
    {
        "$group": {
           "_id": {"col":"$vectors.k", "tableName":"$tableName"},
          "count": {
            "$mergeObjects": "$vectors.v"
          },
        },
      },
    ])
    cta_to_process = list(results)
    for item in tqdm(cta_to_process):
        id_col = item["_id"]["col"]
        tableName = item["_id"]["tableName"]
        types = sorted(item["count"].items(), key=lambda x: x[1], reverse=True)
        if len(types) > 0:
            out[f"{tableName} 0 {id_col}"] = types[0][0]
    return out    

def get_cta_top1(ids_dataset):
    out = {}
    results = cta_c.aggregate([{ "$match": { "idDataset": {"$in": ids_dataset}} },
    {
        "$project": {
            "idDataset": 1,
            "tableName":1,
            "vectors": {
                "$objectToArray": "$winningCandidates"
            }
        },
    },
    { "$unwind": "$vectors" },
    {
        "$group": {
           "_id": {"col":"$vectors.k", "tableName":"$tableName"},
          "count": {
            "$mergeObjects": "$vectors.v"
          },
        },
      },
    ])
    cta_to_process = list(results)
    for item in tqdm(cta_to_process):
        id_col = item["_id"]["col"]
        tableName = item["_id"]["tableName"]
        types = sorted(item["count"].items(), key=lambda x: x[1], reverse=True)
        if len(types) > 0:
            out[f"{tableName} {id_col}"] = types[0][0]
    return out    
    

def get_cta_types_table_to_column(id_dataset, top_k_type=20):
    results = cta_c.aggregate([{ "$match": { "datasetName": id_dataset} },
    {
        "$project": {
            "datasetName": 1,
            "tableName":1,
            "vectors": {
                "$objectToArray": "$winningCandidates"
            }
        },
    },
    { "$unwind": "$vectors" },
    {
        "$group": {
           "_id": {"col":"$vectors.k", "tableName":"$tableName"},
          "count": {
            "$mergeObjects": "$vectors.v"
          },
        },
      },
    ])

    cta_to_process = list(results)
    cta_buffer = {}
    cta_weights = {}
    for item in tqdm(cta_to_process):
        id_col = item["_id"]["col"]
        tableName = item["_id"]["tableName"]
        if tableName not in cta_buffer:
            cta_buffer[tableName] = {}
        if tableName not in cta_weights:
            cta_weights[tableName] = {}    
        cta_weights[tableName][id_col] = item["count"]    
        cta_buffer[tableName][id_col] = sorted(item["count"].items(), key=lambda x: x[1], reverse=True)[0:top_k_type]
        cta_buffer[tableName][id_col] = " ".join([i[0] for i in cta_buffer[tableName][id_col]]) 

    return cta_weights, cta_buffer    


def get_cpa_types_table_to_column(id_dataset, top_k_type=20):
    results = cpa_c.aggregate([{ "$match": { "datasetName": id_dataset} },
    {
        "$project": {
            "datasetName": 1,
            "tableName":1,
            "vectors": {
                "$objectToArray": "$winningCandidates"
            }
        },
    },
    { "$unwind": "$vectors" },
    {
        "$group": {
           "_id": {"col":"$vectors.k", "tableName":"$tableName"},
          "count": {
            "$mergeObjects": "$vectors.v"
          },
        },
      },
    ])

    cpa_to_process = list(results)
    cpa_buffer = {}
    cpa_weights = {}
    for item in tqdm(cpa_to_process):
        id_col = item["_id"]["col"]
        tableName = item["_id"]["tableName"]
        if tableName not in cpa_buffer:
            cpa_buffer[tableName] = {}
        if tableName not in cpa_weights:
            cpa_weights[tableName] = {}    
        cpa_weights[tableName][id_col] = item["count"]     
        cpa_buffer[tableName][id_col] = sorted(item["count"].items(), key=lambda x: x[1], reverse=True)[0:top_k_type]
        cpa_buffer[tableName][id_col] = " ".join([i[0] for i in cpa_buffer[tableName][id_col]]) 

    return cpa_weights, cpa_buffer    


def get_tables_stats(tables_path):
    tables = os.listdir(tables_path)
    stats = []
    for table in tqdm(tables):
        if table.startswith('.'):
            continue
        df = pd.read_csv(f'{tables_path}/{table}')    
        stats.append({"file":table, "row":len(df), "col": len(df.columns)})    
    out = {
        "AVGrow":statistics.mean([x["row"] for x in stats]), 
        "AVGcol":statistics.mean([x["col"] for x in stats]), 
        "maxRow":max([x["row"] for x in stats]),
        "maxCol":max([x["col"] for x in stats])    
    }
    return out


def get_tables_target(tables_path, targets_path_cea, targets_path_cpa, targets_path_cta):
    cea_gt = pd.read_csv(targets_path_cea, sep=',', header=None)
    if targets_path_cpa is not None:
        cpa_gt = pd.read_csv(targets_path_cpa, sep=',', header=None)
    cta_gt = pd.read_csv(targets_path_cta, sep=',', header=None)

    if cea_gt[1].mean()<=cea_gt[2].mean():
        new_columns = list(cea_gt.columns)
        new_columns[1], new_columns[2] = 2, 1
        cea_gt = cea_gt.reindex(new_columns)

    targets_datatype = {}
    for table in os.listdir(tables_path):
        if table.startswith("."):
            continue
        id_table = table[:-4]
        targets_datatype[id_table] = {"SUBJ": 0, "NE": [], "LIT": [], "LIT_DATATYPE": {}}

    for row in tqdm(cea_gt.itertuples(), total=len(cea_gt)):
        id_table, _, id_col = (row[i] for i in range(1, 4)) 
        id_col = int(id_col)
        if id_table not in targets_datatype:
            targets_datatype[id_table] = {"SUBJ": 0, "NE": [], "LIT": [], "LIT_DATATYPE": {}}
        if id_col not in targets_datatype[id_table]["NE"]:     
            targets_datatype[id_table]["NE"].append(id_col)
            
            
    for row in tqdm(cta_gt.itertuples(), total=len(cta_gt)):
        id_table, id_col = (row[i] for i in range(1, 3)) 
        id_col = int(id_col)
        if id_table not in targets_datatype:
            targets_datatype[id_table] = {"SUBJ": id_col, "NE": [], "LIT": [], "LIT_DATATYPE": {}}
        if id_col not in targets_datatype[id_table]["NE"]:     
            targets_datatype[id_table]["NE"].append(id_col)     
            
    if targets_path_cpa is not None:
        for row in cpa_gt.itertuples():
            id_table, id_subj_col, _ = (row[i] for i in range(1, 4))
            if id_table not in targets_datatype:
                targets_datatype[id_table] = {"SUBJ": id_subj_col, "NE": [id_subj_col], "LIT": [], "LIT_DATATYPE": {}}
            targets_datatype[id_table]["SUBJ"] = id_subj_col 


    for id_table in tqdm(targets_datatype):
        try:
            df = pd.read_csv(f"{tables_path}/{id_table}.csv")
            targets_datatype[id_table]["SUBJ"] = targets_datatype[id_table]["NE"][0] 
            compute_datatype(id_table, df, targets_datatype)
        except Exception as e:
            continue
    
    return targets_datatype


def parse_date(str_date):
    date_parsed = None
    
    try:
        int(str_date)
        str_date = f"{str_date}-01-01"
    except:
        pass   
    
    try:
        date_parsed = parse(str_date)
    except:
        pass   
    
    if date_parsed is not None:
        return date_parsed
    
    try:
        str_date = str_date[1:]
        date_parsed = parse(str_date)
    except:
        pass

    if date_parsed is not None:
        return date_parsed
    
    try:
        year = str_date.split("-")[0]
        str_date = f"{year}-01-01"
        date_parsed = parse(str_date)
    except:
        pass

    return date_parsed


def get_cell_datatype(cell):
    try:
        float(cell)
        return "NUMBER"
    except:
        pass
    
    is_a_date = parse_date(cell)
    if is_a_date is not None:
        return "DATETIME"
  
    return "STRING"
   

def compute_datatype(id_table, rows_df, targets_datatype):
    cols_to_datatype = {str(i):{} for i in range(0, len(rows_df.columns))}
    for row in rows_df.iterrows():
        for i, cell in enumerate(row[1]):
            #if i not in targets_datatype[id_table]["NE"]:
            cell_datatype = get_cell_datatype(cell)
            if i not in targets_datatype[id_table]["LIT"]:
                targets_datatype[id_table]["LIT"].append(i)
            i = str(i)
            if cell_datatype not in cols_to_datatype[i]:
                cols_to_datatype[i][cell_datatype] = 0
            cols_to_datatype[i][cell_datatype] += 1    

    for id_col in cols_to_datatype:
        datatype = max(cols_to_datatype[id_col].items(), key=lambda x: x[1], default=(None, 0))
        max_datatype, _ = datatype
        i = int(id_col)
        if i in targets_datatype[id_table]["NE"] and max_datatype != "STRING":
            targets_datatype[id_table]["NE"].remove(i)
        elif i in targets_datatype[id_table]["NE"]:
            targets_datatype[id_table]["LIT"].remove(i)
            continue
        targets_datatype[id_table]["LIT_DATATYPE"][id_col] = max_datatype 
       

def get_cell_tables_data_to_annotations(targets_path_cea, tables_path, buffer):
    tables = os.listdir(tables_path)
    cea_ann_gt = get_cea_ann(targets_path_cea)
    for table in tqdm(tables):
        name = table[:-4]
        if table.startswith('.'):
            continue
        id_row = 1
        df = pd.read_csv(f'{tables_path}/{table}')   
        json_data = json.loads(df.to_json(orient='split'))
        rows = json_data["data"]
        for row in rows:
            for id_col, cell in enumerate(row):
                cell = ' '.join(str(cell).lower().split())
                key = f"{name} {id_row} {id_col}"
                if key in cea_ann_gt:
                    if cell not in buffer:
                        buffer[cell] = {}
                    entities = [e for e in cea_ann_gt[key]]
                    for entity in entities:
                        if entity not in buffer[cell]:
                            buffer[cell][entity] = True    
            id_row += 1

            
def get_key_to_cell(tables_path):
    tables = os.listdir(tables_path)
    buffer = {}
    for table in tqdm(tables):
        name = table[:-4]
        if table.startswith('.'):
            continue
        id_row = 1
        df = pd.read_csv(f'{tables_path}/{table}')   
        json_data = json.loads(df.to_json(orient='split'))
        rows = json_data["data"]
        for row in rows:
            for id_col, cell in enumerate(row):
                cell = ' '.join(str(cell).lower().split())
                key = f"{name} {id_row} {id_col}"
                buffer[key] = cell
            id_row += 1     
    return buffer            
            
    
def get_cells_set(tables_path, targets_datatype):
    cells_set = {}
    for id_table in tqdm(targets_datatype):
        df = pd.read_csv(f"{tables_path}/{id_table}.csv")
        for row in df.iterrows():
            for i, cell in enumerate(row[1]):
                if i in targets_datatype[id_table]["NE"]:
                    cell = str(cell)
                    cell = ' '.join(cell.split()).lower()
                    cells_set[cell] = True
    return cells_set    


def make_buffer(id_dataset, tables_path, cea_target_path, cpa_target_path, cta_target_path, kg_reference="wikidata", candidate_size=100):
    targets_datatype = get_tables_target(tables_path, cea_target_path, cpa_target_path, cta_target_path)  
    tables = os.listdir(tables_path)
    tables_buffer = []

    for table in tqdm(tables):
        name = table[:-4]
        if table.startswith('.'):
            continue
        id_row = 1
        df = pd.read_csv(f'{tables_path}/{table}')    
        df.fillna("",inplace=True)
        json_data = json.loads(df.to_json(orient='split'))
        table = {
            "datasetName": id_dataset,
            "tableName": name,
            "header": list(df.columns),
            "rows": [],
            "semanticAnnotations": {},
            "metadata": {"column": {}},
            "kgReference": kg_reference,
            "candidateSize": candidate_size
        }

        column = []
        datatype = targets_datatype[table["tableName"]]
        for id_col, _ in enumerate(json_data["data"][0]):
            if id_col in datatype["NE"]: 
                column.append({"idColumn": id_col, "tag": "NE" if id_col != datatype["SUBJ"] else "SUBJ"})
            elif id_col in datatype["LIT"]:
                column.append({"idColumn": id_col, "tag": "LIT", "datatype": datatype["LIT_DATATYPE"][str(id_col)]})
        table["metadata"]["column"] = column 
        rows = json_data["data"]    
        for row in rows:
            table["rows"].append({"idRow": id_row, "data": [str(cell) for cell in row]})
            id_row += 1
        tables_buffer.append(table)        

    return tables_buffer


def make_buffer_base(id_dataset, tables_path, kg_reference="wikidata", candidate_size=100):
    tables = os.listdir(tables_path)
    tables_buffer = []

    for table in tqdm(tables):
        name = table[:-4]
        if table.startswith('.'):
            continue
        id_row = 1
        df = pd.read_csv(f'{tables_path}/{table}')    
        df.fillna("",inplace=True)
        json_data = json.loads(df.to_json(orient='split'))
        table = {
            "datasetName": id_dataset,
            "tableName": name,
            "header": list(df.columns),
            "rows": [],
            "semanticAnnotations": {},
            "metadata": {"column": {}},
            "kgReference": kg_reference,
            "candidateSize": candidate_size
        }

        rows = json_data["data"]    
        for row in rows:
            table["rows"].append({"idRow": id_row, "data": [str(cell) for cell in row]})
            id_row += 1
        tables_buffer.append(table)        

    return tables_buffer

def make_buffer_with_cta(id_dataset_old, id_dataset, tables_path, cea_target_path, cpa_target_path, cta_target_path, kg_reference="wikidata", candidate_size=100):
    targets_datatype = get_tables_target(tables_path, cea_target_path, cpa_target_path, cta_target_path)  
    tables = os.listdir(tables_path)
    tables_buffer = []
    cta_weights, cta_buffer = get_cta_types_table_to_column(id_dataset_old, top_k_type=5)
    for table in tqdm(tables):
        name = table[:-4]
        if table.startswith('.'):
            continue
        id_row = 1
        df = pd.read_csv(f'{tables_path}/{table}')    
        df.fillna("",inplace=True)
        json_data = json.loads(df.to_json(orient='split'))
        table = {
            "datasetName": id_dataset,
            "tableName": name,
            "header": list(df.columns),
            "rows": [],
            "semanticAnnotations": {"cta": []},
            "metadata": {"column": {}},
            "kgReference": kg_reference,
            "candidateSize": candidate_size
        }

        column = []
        datatype = targets_datatype[table["tableName"]]
        for id_col, _ in enumerate(json_data["data"][0]):
            if id_col in datatype["NE"]: 
                column.append({"idColumn": id_col, "tag": "NE" if id_col != datatype["SUBJ"] else "SUBJ"})
                if name in cta_buffer and cta_buffer[name].get(str(id_col)) is not None and len(cta_buffer[name].get(str(id_col))) > 0:
                    table["semanticAnnotations"]["cta"].append({
                        "idColumn": id_col, 
                        "types": cta_buffer[name][str(id_col)].split(" ")
                    })
            elif id_col in datatype["LIT"]:
                column.append({"idColumn": id_col, "tag": "LIT", "datatype": datatype["LIT_DATATYPE"][str(id_col)]})
        table["metadata"]["column"] = column 
        rows = json_data["data"]    
        for row in rows:
            table["rows"].append({"idRow": id_row, "data": [str(cell) for cell in row]})
            id_row += 1
        tables_buffer.append(table)        
    
    return tables_buffer


def make_buffer_for_missing_entities(id_dataset_old, id_dataset, tables_path, cea_target_path, cpa_target_path, cta_target_path, candidate_size=100):
    if cpa_target_path is not None:
        targets_datatype = get_tables_target(tables_path, cea_target_path, cpa_target_path, cta_target_path)  
    else:
        targets_datatype = get_tables_target2(tables_path, cea_target_path, cta_target_path)  
    cta_weights, cta_buffer = get_cta_types_table_to_column(id_dataset_old)
    cpa_weights, cpa_buffer = get_cpa_types_table_to_column(id_dataset_old)

    results = cea_c.find({"idDataset": id_dataset_old})
    total = cea_c.count_documents({"idDataset": id_dataset_old})
    tables_buffer = {}
    for result in tqdm(results, total=total):
        id_table = result["tableName"]
        name = id_table
        if id_table not in tables_buffer:
            tables_buffer[id_table] = {
                "datasetName": id_table, 
                "tableName": id_dataset,
                "rows": [],
                "semanticAnnotations": {"cta": [], "cpa": []},
                "metadata": {"column": {}},
                "kgReference": "wikidata",
                "candidateSize": candidate_size
            }
            datatype = targets_datatype[id_table]
            column = []
            for id_col, _ in enumerate(result["winningCandidates"]):
                if id_col in datatype["NE"]: 
                    column.append({"idColumn": id_col, "tag": "NE" if id_col != datatype["SUBJ"] else "SUBJ"})
                    if name in cta_buffer and cta_buffer[name].get(str(id_col)) is not None and len(cta_buffer[name].get(str(id_col))) > 0:
                        tables_buffer[id_table]["semanticAnnotations"]["cta"].append({
                                                                    "idColumn": id_col, 
                                                                    "types": cta_buffer[name][str(id_col)].split(" "),
                                                                    "typesWeights": cta_weights[name][str(id_col)]
                                                                    })
                else:
                    column.append({"idColumn": id_col, "tag": "LIT", "datatype": datatype["LIT_DATATYPE"][str(id_col)]})
                if name in cpa_buffer and cpa_buffer[name].get(str(id_col)) is not None and len(cpa_buffer[name].get(str(id_col))) > 0:
                    tables_buffer[id_table]["semanticAnnotations"]["cpa"].append({
                                                                "idColumn": id_col, 
                                                                "predicates": cpa_buffer[name][str(id_col)].split(" "),
                                                                "predicatesWeights": cpa_weights[name][str(id_col)]
                                                                })    
            tables_buffer[id_table]["metadata"]["column"] = column        
        id_row = result["row"]
        match = True
        for column in result["winningCandidates"]:
            if len(column) > 1 or (len(column) == 1 and column[0]["score"] < 1):
                match = False
                break
        if not match: 
            tables_buffer[id_table]["rows"].append({"idRow": id_row, "data": result["data"]})
    buffer = []
    for id_table in tables_buffer:
        if len(tables_buffer[id_table]["rows"]) > 0:
            buffer.append(tables_buffer[id_table])        
    new_buffer = []
    sample_size = 25
    for table in tqdm(buffer):
        if len(table["rows"]) > sample_size:
            rows = []
            id_dataset = table["dataset"]
            id_table = table["name"]
            semantic_annotations = table["semanticAnnotations"]
            metadata = table["metadata"]
            for i, row in enumerate(table["rows"]):
                rows.append(row)
                if len(rows) >= sample_size and (len(table["rows"]) - i) > 25:
                    new_buffer.append({
                        "name": id_table, 
                        "dataset": id_dataset,
                        "rows": rows,
                        "semanticAnnotations": semantic_annotations,
                        "metadata": metadata,
                        "kgReference": "wikidata",
                        "candidateSize": candidate_size
                    })
                    rows = []
            if len(rows) > 0:
                new_buffer.append({
                    "name": id_table, 
                    "dataset": id_dataset,
                    "rows": rows,
                    "semanticAnnotations": semantic_annotations,
                    "metadata": {"column": {}},
                    "kgReference": "wikidata",
                    "candidateSize": candidate_size
                })
                rows = []
        else:
            new_buffer.append(table)        
    return new_buffer


def get_stats_on_dataset(cea_target_path, cpa_target_path, cta_target_path, tables_path):
    n_tables, total_rows, total_columns  = (0, 0, 0)
    for table in tqdm(os.listdir(tables_path)):
        if table.startswith("."):
            continue
        data = pd.read_csv(f"{tables_path}/{table}")    
        total_rows += len(data)
        total_columns += len(data.columns)
        n_tables += 1
        
    avg_columns = round(total_columns/n_tables, 2)
    avg_rows = round(total_rows/n_tables, 2)
    cea_target = pd.read_csv(cea_target_path) 
    if cpa_target_path is not None:
        cpa_target = pd.read_csv(cpa_target_path) 
    else:
        cpa_target = ""
    cta_target = pd.read_csv(cta_target_path) 
    entities, predicates, classes  = (len(cea_target), len(cpa_target), len(cta_target))
    return {
        "tables": n_tables, 
        "columns":total_columns, 
        "avg_columns": avg_columns,
        "rows":total_rows, 
        "avg_rows": avg_rows,
        "classes":classes, 
        "entities":entities, 
        "predicates":predicates
    }
