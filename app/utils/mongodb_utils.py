from pymongo import MongoClient
from typing import List, Dict

class MongoDBHandler:
    def __init__(self, uri: str, db_name: str):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]

    def create_dataset(self, dataset_name: str):
        if dataset_name not in self.db.list_collection_names():
            self.db.create_collection(dataset_name)
        return {"message": f"Dataset '{dataset_name}' created successfully"}

    def get_datasets(self) -> List[str]:
        return self.db.list_collection_names()

    def get_dataset(self, dataset_name: str) -> Dict:
        if dataset_name in self.db.list_collection_names():
            dataset = self.db[dataset_name]
            tables = dataset.find({}, {"_id": 0})
            return {"name": dataset_name, "tables": list(tables)}
        else:
            raise ValueError("Dataset not found")

    def delete_dataset(self, dataset_name: str):
        if dataset_name in self.db.list_collection_names():
            self.db[dataset_name].drop()
            return {"message": f"Dataset '{dataset_name}' deleted successfully"}
        else:
            raise ValueError("Dataset not found")

    def create_table(self, dataset_name: str, table_name: str, schema: Dict):
        collection = self.db[dataset_name]
        collection.insert_one({"table_name": table_name, "schema": schema, "rows": []})
        return {"message": f"Table '{table_name}' created in dataset '{dataset_name}'"}

    def get_tables(self, dataset_name: str) -> List[Dict]:
        collection = self.db[dataset_name]
        tables = collection.find({}, {"_id": 0, "table_name": 1, "schema": 1})
        return list(tables)

    def get_table(self, dataset_name: str, table_name: str) -> Dict:
        collection = self.db[dataset_name]
        table = collection.find_one({"table_name": table_name}, {"_id": 0})
        if table:
            return table
        else:
            raise ValueError("Table not found")

    def delete_table(self, dataset_name: str, table_name: str):
        collection = self.db[dataset_name]
        result = collection.delete_one({"table_name": table_name})
        if result.deleted_count:
            return {"message": f"Table '{table_name}' deleted successfully"}
        else:
            raise ValueError("Table not found")

    def add_row_to_table(self, dataset_name: str, table_name: str, row: Dict):
        collection = self.db[dataset_name]
        collection.update_one({"table_name": table_name}, {"$push": {"rows": row}})
        return {"message": "Row added successfully"}