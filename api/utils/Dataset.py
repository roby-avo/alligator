import math
import os

class DatasetModel:

    config_values = os.environ["CONFIG_VALUES"].split(",")  # CONFIG_VALUES
    DATASET_FOR_PAGE = int(config_values[0])
    
    def __init__(self, db, table_metadata):
        self._db = db
        self._table_metadata = table_metadata

    def store_datasets(self):
        dataset_c = self._db.get_collection("dataset")
        total_datasets = dataset_c.estimated_document_count()
        page = math.floor(total_datasets / DatasetModel.DATASET_FOR_PAGE) + 1

        for dataset_name in self._table_metadata:
            n_tables = len(self._table_metadata[dataset_name])
            result = dataset_c.find_one({"datasetName": dataset_name})
            if result is None:
                dataset_c.insert_one({
                    "datasetName": dataset_name,
                    "Ntables": n_tables,
                    "status": {
                        "TODO":n_tables, 
                        "DOING": 0, 
                        "DONE": 0
                    },
                    "%": 0,
                    "page": page,
                    "process": "TODO"
                })
            else:
                TODO = result["status"]["TODO"] + n_tables
                dataset_c.update_one({"_id": result['_id']}, {"$set": {"status.TODO": TODO}})  