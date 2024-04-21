import os
import json
import requests
import pandas as pd
from tqdm import tqdm

def clean_str(value):
    value = str(value)
    stop_charaters = ["_"]
    for char in stop_charaters:
        value = value.replace(char, " ")
    value = " ".join(value.split()).lower()
    return value

def cea_process(cea_target_path):
    minimum_row_is_zero = False
    cea_gt = pd.read_csv(cea_target_path, sep=',', header=None)
    NE_cols = set() # to store couple of (id_table, id_col) for tracing NE cols to be annotated!
    cell_to_entity = {}
    if cea_gt[1].mean()<=cea_gt[2].mean(): # check to indentify if the excpected order is id_row, id_col otherwise swap!
        new_columns = list(cea_gt.columns)
        new_columns[1], new_columns[2] = 2, 1
        cea_gt = cea_gt.reindex(new_columns)
    
    for i, row in tqdm(cea_gt.iterrows(), total=len(cea_gt)):
        id_table, id_row, id_col, entity = row
        cell_to_entity[f"{id_table} {id_row} {id_col}"] = entity
        NE_cols.add(f"{id_table} {id_col}")
        if id_row == 0:
            minimum_row_is_zero = True

    return cell_to_entity, NE_cols, minimum_row_is_zero

def generate_api_format(id_dataset, tables_path, cell_to_entity, NE_cols, minimum_row_is_zero):
    tables = os.listdir(tables_path)
    buffer = []
    candidates_to_be_covered = {}
    key_to_cell = {}
    for table in tqdm(tables):
        name = table[:-4]
        if table.startswith('.'):
            continue
        id_row = 1
        df = pd.read_csv(
            f'{tables_path}/{table}',
            sep=',',             # Change delimiter if different
            quotechar='"',       # Ensure correct quote character
            on_bad_lines='skip'  # Skip lines with too many/few fields
        )
        for column in df.columns:
            if df[column].dtype == 'float64':
                # For float columns, fill with 0 or another appropriate value like df[column].mean()
                df[column].fillna(0, inplace=True)
            else:
                # For object columns, fill with an empty string or another appropriate placeholder
                df[column].fillna("", inplace=True)
        json_data = json.loads(df.to_json(orient='split'))
        table = {
            "datasetName": id_dataset,
            "tableName": name,
            "header": list(df.columns),
            "rows": [],
            "semanticAnnotations": {},
            "metadata": {"column": []},
            "kgReference": "wikidata",
            "candidateSize": 100
        }
        rows = json_data["data"]
        id_row = 1
        if minimum_row_is_zero:
            id_row = 0
        for row in rows:
            table["rows"].append({"idRow": id_row, "data": [str(cell) for cell in row]})
            id_row += 1
    
        for id_col, row in enumerate(rows[0]):
            key = f"{name} {id_col}"
            if key in NE_cols:
                table["metadata"]["column"].append({"idColumn": id_col, "tag": "NE"})
    
        buffer.append(table)
        for id_row, row in enumerate(rows):
            for id_col, cell in enumerate(row):
                key = f"{name} {id_row} {id_col}"
                key_to_cell[key] = cell
    
    for key in cell_to_entity:
        name, id_row, id_col = key.split(" ")
        if not minimum_row_is_zero:
            id_row = int(id_row)
            id_row -= 1
        new_key = f"{name} {id_row} {id_col}"
        cell = key_to_cell[new_key]
        if cell not in candidates_to_be_covered:
            candidates_to_be_covered[cell] = set()
        candidates_to_be_covered.add(clean_str(cell_to_entity[key]))
    
    for cell in candidates_to_be_covered:
        candidates_to_be_covered[cell] = list(candidates_to_be_covered[cell])

    return buffer, candidates_to_be_covered



datasets = {
    "Round1_T2D": {
        "tables": "/data/my-data/Dataset/Round1_T2D/tables",
        "cea":"/data/my-data/Dataset/Round1_T2D/gt/cea.csv",
        "cpa":"/data/my-data/Dataset/Round1_T2D/gt/CPA_Round1_gt.csv",
        "cta": "/data/my-data/Dataset/Round1_T2D/gt/CTA_Round1_gt.csv"
    },
    "Round3": {
        "tables": "/data/my-data/Dataset/Round3_2019/tables/",
        "cea":"/data/my-data/Dataset/Round3_2019/gt/cea.csv",
        "cpa":"/data/my-data/Dataset/Round3_2019/gt/CPA_Round3_gt.csv",
        "cta": "/data/my-data/Dataset/Round3_2019/gt/CTA_Round3_gt.csv"
    },
    "Round4": {
        "tables": "/data/my-data/Dataset/Round4_2020/tables/",
        "cea":"/data/my-data/Dataset/Round4_2020/gt/cea.csv",
        "cpa":"/data/my-data/Dataset/Round4_2020/gt/cpa.csv",
        "cta": "/data/my-data/Dataset/Round4_2020/gt/cta.csv"
    },
    "2T-2020": {
        "tables":"/data/my-data/Dataset/2T_Round4/tables/", 
        "cea":"/data/my-data/Dataset/2T_Round4/gt/cea.csv",
        "cpa": None,
        "cta": "/data/my-data/Dataset/2T_Round4/gt/cta.csv"
    },
    "HardTableR2-2021": {
        "tables":"/data/my-data/Dataset/HardTablesR2/tables/", 
        "cea":"/data/my-data/Dataset/HardTablesR2/gt/cea.csv",
        "cpa":"/data/my-data/Dataset/HardTablesR2/gt/cpa.csv",
        "cta": "/data/my-data/Dataset/HardTablesR2/gt/cta.csv"
    },
    "HardTableR3-2021": {
        "tables":"/data/my-data/Dataset/HardTablesR3/tables/", 
        "cea":"/data/my-data/Dataset/HardTablesR3/gt/cea.csv",
        "cpa":"/data/my-data/Dataset/HardTablesR3/gt/cpa.csv",
        "cta": "/data/my-data/Dataset/HardTablesR3/gt/cta.csv"
    }
}


global_candidates_to_be_covered = {}
for dataset in datasets:
    tables_path, cea_target_path, cpa_target_path, cta_target_path = list(datasets[dataset].values())
    cell_to_entity, NE_cols, minimum_row_is_zero = cea_process(cea_target_path)
    buffer, candidates_to_be_covered = generate_api_format(dataset, tables_path, cell_to_entity, NE_cols, minimum_row_is_zero)
    for cell in candidates_to_be_covered:
        if cell not in global_candidates_to_be_covered:
            global_candidates_to_be_covered[cell] = set()
        global_candidates_to_be_covered[cell].update(candidates_to_be_covered[cell])


for cell in global_candidates_to_be_covered:
    global_candidates_to_be_covered[cell] = list(global_candidates_to_be_covered[cell])
