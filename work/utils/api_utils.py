import copy
import math
import numpy as np

DATASET_FOR_PAGE = 5
TABLE_FOR_PAGE = 10
SAMPLE_SIZE = 25

def fill_tables(tables, row_c):
    new_tables = []
    dataset = {}
    for table in tables:
        column_metadata = table.get('metadata', {}).get('column', {}) 
        column_types = table.get('semanticAnnotations', {}).get('cta', {})
        column_predicates = table.get('semanticAnnotations', {}).get('cpa', {})
        result = row_c.find_one({"datasetName": table["datasetName"], "tableName": table["tableName"]})
        page = 1 if result is None else result["page"] + 1
        table['page'] = page
        table['column'] = {str(c['idColumn']):c['tag'] for c in column_metadata}
        table['target'] = {"SUBJ": 0, "NE": [], "LIT": [], "LIT_DATATYPE": {}}
        table['types'] = {str(c['idColumn']):' '.join(sorted(c['types'], reverse=True)) for c in column_types}
        
        """
        table['typesWeights'] = {}
        for c in column_types:
            table['typesWeights'][str(c['idColumn'])] = c['typesWeights'] 
            del c['typesWeights'] 
        
        table['predicatesWeights'] = {}
        for c in column_predicates:
            table['predicatesWeights'][str(c['idColumn'])] = c['predicatesWeights'] 
            del c['predicatesWeights'] 
        """
        
        for id_col, column in enumerate(column_metadata):
            if column["tag"] == "SUBJ":
                table['target']['SUBJ'] = id_col
                table['target']['NE'].append(column["idColumn"]) 
            elif column["tag"] == "NE":
                table['target']['NE'].append(column["idColumn"]) 
            elif column["tag"] == "LIT":
                table['target']['LIT'].append(column["idColumn"]) 
                table['target']['LIT_DATATYPE'][str(column["idColumn"])] = column["datatype"]
        table['status'] = 'TODO'
        table['state'] = 'READY'
        dataset_name = table['datasetName']
        table_name = table['tableName']
        if dataset_name not in dataset:
            dataset[dataset_name] = {}
        if table_name not in dataset[dataset_name]:
            dataset[dataset_name][table_name] = {
                "datasetName": dataset_name,
                "tableName": table_name,
                "Nrows": 0, 
                "status": {
                    "TODO": 0, 
                    "DOING": 0, 
                    "DONE": 0
                },
                "statusCopy": {
                    "TODO": 0, 
                    "DOING": 0, 
                    "DONE": 0
                },
                "state": "TODO"
            }  
        dataset[dataset_name][table_name]["status"]["TODO"] += 1 
        dataset[dataset_name][table_name]["Nrows"] += len(table["rows"])
        if "candidateSize" not in table:
            table['candidateSize'] = 100

        if len(table["rows"]) > 25: 
            dataset_name, table_name, header, rows = [table[key] for key in ["datasetName", "tableName", "header", "rows"]]
            rows = np.array_split(rows, math.floor(len(rows)/SAMPLE_SIZE))
            del table["datasetName"]
            del table["tableName"] 
            del table["header"]
            del table["rows"]
            for row in rows: 
                new_tables.append(dict(
                    **{
                        "datasetName": dataset_name,
                        "tableName": table_name,
                        "header": header,
                        "rows": row.tolist()
                    },
                    **table
                ))
        else:
            new_tables.append(table)        

    return dataset, new_tables


def fill_dataset(dataset, dataset_c, table_c):
    total_datasets = dataset_c.estimated_document_count()
    id_dataset = total_datasets 
    page = math.floor(total_datasets / DATASET_FOR_PAGE) + 1
    for name in dataset:
        result = dataset_c.find_one({"datasetName": name})
        if result is None:
            dataset_c.insert_one({
                "datasetName": name,
                "Ntables": len(dataset[name]),
                "status": {
                    "TODO":len(dataset[name]), 
                    "DOING": 0, 
                    "DONE": 0
                },
                "statusCopy": {
                    "TODO": 0, 
                    "DOING": 0, 
                    "DONE": 0
                },
                "%": 0,
                "page": page
            })
            id_dataset += 1
        else:
            TODO = result["status"]["TODO"] + len(dataset[name])
            dataset_c.update_one({"_id": result['_id']}, {"$set": {"status.TODO": TODO}})  
        fill_table(dataset[name], table_c)
        

def fill_table(tables, table_c):
    table_to_id_table = {}
    for table_name in tables:
        dataset_name = tables[table_name]["datasetName"]
        total_tables = table_c.count_documents({"datasetName": dataset_name})
        page = math.floor(total_tables / TABLE_FOR_PAGE) + 1
        tables[table_name]["page"] = page
        table_c.insert_one(tables[table_name])
    
    return table_to_id_table


def format_table(table_name, dataset_name, table, header, kg_reference="wikidata", json=False):
    rows = []
    if not json:
        for id_row, row in enumerate(table):
            rows.append({"idRow":id_row+1, "data":row})
    else:
        rows = table        
    buffer = []
    if len(rows) > 25:
        rows = np.array_split(rows, math.floor(len(rows)/SAMPLE_SIZE))
        for item in rows:      
            object = {
                "tableName": table_name,
                "datasetName": dataset_name,
                "header": header,
                "rows": item.tolist(),
                "kgReference": kg_reference
            }     
            buffer.append(object)
    else: 
        object = {
            "tableName": table_name,
            "datasetName": dataset_name,
            "header": header,
            "rows": rows,
            "kgReference": kg_reference
        }     
        buffer.append(object)       
    
    return buffer