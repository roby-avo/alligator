from pymongo import MongoClient
import os

MONGO_ENDPOINT, MONGO_ENDPOINT_PORT = os.environ['MONGO_ENDPOINT'].split(":")
MONGO_ENDPOINT_USERNAME = os.environ['MONGO_INITDB_ROOT_USERNAME']
MONGO_ENDPOINT_PASSWORD = os.environ['MONGO_INITDB_ROOT_PASSWORD']
MONGO_DBNAME = os.environ['MONGO_DBNAME']

class MongoDBWrapper:
    def __init__(self, database_name):
        """
        Initialize the MongoDBWrapper.

        :param database_name: The name of the database.
        :param host: MongoDB host (default is 'localhost').
        :param port: MongoDB port (default is 27017).
        """
        self.client = MongoClient(
                            MONGO_ENDPOINT, 
                            int(MONGO_ENDPOINT_PORT), 
                            username=MONGO_ENDPOINT_USERNAME, 
                            password=MONGO_ENDPOINT_PASSWORD, 
                            authSource='admin'
                        )
        self.database = self.client[database_name]

    def get_collection(self, collection_name):
        """
        Access the specified collection.

        :param collection_name: The name of the collection to access.
        :return: Collection object.
        """
        return self.database[collection_name]

    def insert(self, collection_name, data):
        """
        Insert data into the specified collection.

        :param collection_name: Name of the collection.
        :param data: Data to insert (either a dictionary or a list of dictionaries).
        :return: Inserted IDs.
        """
        collection = self.get_collection(collection_name)
        if isinstance(data, list):
            return collection.insert_many(data).inserted_ids
        else:
            return collection.insert_one(data).inserted_id

    def find(self, collection_name, query=None):
        """
        Find documents in the specified collection.

        :param collection_name: Name of the collection.
        :param query: Query criteria (default is None, which returns all documents).
        :return: List of documents matching the query.
        """
        collection = self.get_collection(collection_name)
        return list(collection.find(query))

    def update(self, collection_name, query, new_values):
        """
        Update documents in the specified collection.

        :param collection_name: Name of the collection.
        :param query: Query criteria.
        :param new_values: New values to set.
        :return: Modified count.
        """
        collection = self.get_collection(collection_name)
        return collection.update_many(query, {'$set': new_values}).modified_count

    def delete(self, collection_name, query):
        """
        Delete documents from the specified collection.

        :param collection_name: Name of the collection.
        :param query: Query criteria.
        :return: Deleted count.
        """
        collection = self.get_collection(collection_name)
        return collection.delete_many(query).deleted_count

    def close(self):
        """
        Close the database connection.
        """
        self.client.close()

# Usage example:
# db = MongoDBWrapper("mydb")
# db.insert("mycollection", {"name": "Alice", "age": 30})
# print(db.find("mycollection", {"name": "Alice"}))
# db.close()
