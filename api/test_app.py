import os
import unittest
from datetime import datetime, timedelta
from flask_testing import TestCase
from dotenv import load_dotenv
from pymongo import MongoClient
from app import app, mongoDBWrapper

# Load environment variables from .env file
load_dotenv()

class TestAPI(TestCase):

    def create_app(self):
        app.config['TESTING'] = True
        app.config['MONGO_URI'] = os.getenv("MONGO_URI_TEST")
        app.config['API_TOKEN'] = os.getenv("ALLIGATOR_TOKEN")
        app.config['UNLIMITED_TOKEN'] = os.getenv("ALLIGATOR_TOKEN_SECRET")
        app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 500MB limit
        return app

    def setUp(self):
        # Clear the collections before each test
        mongoDBWrapper.get_collection("dataset").delete_many({})
        mongoDBWrapper.get_collection("table").delete_many({})
        mongoDBWrapper.get_collection("row").delete_many({})
        mongoDBWrapper.get_collection("rateLimit").delete_many({})

    def test_token_validation(self):
        response = self.client.get('/dataset', query_string={'token': 'invalid_token'})
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json['Error'], 'Invalid Token')

    def test_delete_dataset(self):
        self.client.post('/dataset', query_string={'token': os.getenv("ALLIGATOR_TOKEN_SECRET")}, json={'datasetName': 'test_dataset'})
        response = self.client.delete('/dataset/test_dataset', query_string={'token': os.getenv("ALLIGATOR_TOKEN_SECRET")})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['deleted'], True)

    def test_create_dataset(self):
        response = self.client.post('/dataset', query_string={
            'token': os.getenv("ALLIGATOR_TOKEN_SECRET"),
            'datasetName': 'test_dataset'
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['message'], 'Created dataset test_dataset')

    def test_get_dataset(self):
        # Create the dataset first
        self.client.post('/dataset', query_string={
            'token': os.getenv("ALLIGATOR_TOKEN_SECRET"),
            'datasetName': 'test_dataset'
        })
        
        # Retrieve the dataset with pagination
        response = self.client.get('/dataset', query_string={
            'token': os.getenv("ALLIGATOR_TOKEN_SECRET"),
            'page': 1
        })
        
        self.assertEqual(response.status_code, 200)
        self.assertTrue('data' in response.json)

    def test_create_with_array(self):
        data = [
            {
                "datasetName": "Dataset1",
                "tableName": "Test1",
                "header": ["col1", "col2", "col3"],
                "rows": [
                    {"idRow": 1, "data": ["A", "B", "C"]},
                    {"idRow": 2, "data": ["D", "E", "F"]}
                ],
                "semanticAnnotations": {"cea": [], "cta": [], "cpa": []},
                "metadata": {"columnMetadata": []},
                "kgReference": "wikidata"
            }
        ]
        response = self.client.post('/dataset/createWithArray', query_string={'token': os.getenv("ALLIGATOR_TOKEN_SECRET")}, json=data)
        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.json['status'], 'Ok')


    def test_rate_limiting(self):
        token = os.getenv("ALLIGATOR_TOKEN")
        for _ in range(1000):
            self.client.get('/dataset', query_string={'token': token})
        response = self.client.get('/dataset', query_string={'token': token})
        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.json['Error'], 'Rate limit exceeded')

if __name__ == '__main__':
    unittest.main()
