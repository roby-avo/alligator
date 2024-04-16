# Import necessary packages and modules
import os  # For interacting with the operating system
import math  # For mathematical operations
import traceback  # To provide details of exceptions
import pandas as pd  # Popular data manipulation package
import redis  # Redis database interface
from flask import Flask, request, jsonify  # Flask web framework components
from flask_cors import CORS  # To handle Cross-Origin Resource Sharing (CORS)
from flask_restx import Api, Resource, fields, reqparse  # Extensions for Flask to ease REST API development
from werkzeug.datastructures import FileStorage  # To handle file storage in Flask
import json  # For JSON data manipulation
import datetime  # For date and time operations
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import geoip2.database
import logging

# Disable TensorFlow warnings
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'  # This hides info and warning messages

# Or, to suppress all TensorFlow messages (including errors)
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

# Additionally, to suppress Python warnings
logging.getLogger('tensorflow').setLevel(logging.ERROR)

from process.wrapper.Database import MongoDBWrapper  # MongoDB database wrapper
from utils.Dataset import DatasetModel  # Dataset utility model
from utils.Table import TableModel  # Table utility model
import pymongo  # MongoDB database interface


# Retrieve environment variables for Redis configuration and API token
REDIS_ENDPOINT = os.environ["REDIS_ENDPOINT"]  # Endpoint for Redis connection
REDIS_JOB_DB = int(os.environ["REDIS_JOB_DB"])  # Redis database number for jobs

API_TOKEN = os.environ["ALLIGATOR_TOKEN"]  # API token for authentication

# Initialize Redis client for tracking active jobs
job_active = redis.Redis(host=REDIS_ENDPOINT, db=REDIS_JOB_DB)

# Initialize MongoDB wrapper and get collections for different data models
mongoDBWrapper = MongoDBWrapper()
row_c = mongoDBWrapper.get_collection("row")
cea_prelinking_c = mongoDBWrapper.get_collection("ceaPrelinking")
candidate_scored_c = mongoDBWrapper.get_collection("candidateScored")
cea_c = mongoDBWrapper.get_collection("cea")
cpa_c = mongoDBWrapper.get_collection("cpa")
cta_c = mongoDBWrapper.get_collection("cta")
dataset_c = mongoDBWrapper.get_collection("dataset")
table_c = mongoDBWrapper.get_collection("table")
ip_logs_c = mongoDBWrapper.get_collection("ipLogs")

# Initialize Flask application and enable CORS
app = Flask(__name__)
CORS(app)

# Read API description from a text file
with open("data.txt") as f:
    description = f.read()

# Set up the API with version, title, and description read from the file
api = Api(app, version="1.0", title="Alligator", description=description)

# Define a namespace for dataset related operations
ds = api.namespace("dataset", description="Dataset namespace")

# Initialize a parser for file uploads
upload_parser = api.parser()
upload_parser.add_argument("file", location="files",
                           type=FileStorage, required=True)

ALLIGATOR_TOKEN_SECRET = os.environ["ALLIGATOR_TOKEN_SECRET"]
# Define a function to validate the provided token against the expected API token
def validate_token(token, bypass=False):
    if bypass:
        return token == ALLIGATOR_TOKEN_SECRET
    return token == API_TOKEN


# Define data models for the API to serialize and deserialize data
rows_fields = api.model("Rows", {
    "idRow": fields.Integer,
    "data": fields.List(fields.String)
})

cta_fields = api.model("Cta", {
    "idColumn": fields.Integer,
    "types": fields.List(fields.String),
})

cpa_fields = api.model("Cpa", {
    "idSourceColumn": fields.Integer,
    "idTargetColumn": fields.Integer,
    "predicate": fields.List(fields.String)
})

cea_fields = api.model("Cea", {
    "idColumn": fields.Integer,
    "idRow": fields.Integer,
    "entity": fields.List(fields.String)
})

semantic_annotation_fields = api.model("SemanticAnnotation", {
    "cta": fields.Nested(cta_fields),
    "cpa": fields.Nested(cpa_fields),
    "cea": fields.Nested(cea_fields),
})

column_fields = api.model("Column", {
    "idColumn": fields.Integer,
    "tag": fields.String
})

metadata = api.model("Metadata", {
    "columnMetadata": fields.List(fields.Nested(column_fields))
})

table_fields = api.model("Table", {
    "name": fields.String,
    "header": fields.List(fields.String),
    "rows": fields.List(fields.Nested(rows_fields)),
    "semanticAnnotations": fields.Nested(semantic_annotation_fields),
    "metadata": fields.Nested(metadata),
    "kgReference": fields.String,
    "candidateSize": fields.Integer
}) 

table_list_field = api.model("TablesList",  {
    "datasetName": fields.String(required=True, example="Dataset1"),
    "tableName": fields.String(required=True, example="Test1"),
    "header": fields.List(fields.String(), required=True, example=["col1", "col2", "col3", "Date of Birth"]),
    "rows": fields.List(fields.String(), required=True, example=[
        { "idRow": 1, "data": ["Zooey Deschanel", "Los Angeles", "United States", "January 17, 1980"]},
        { "idRow": 2, "data": ["Sarah Mclachlan", "Halifax", "Canada", "January 28, 1968"]},
        { "idRow": 3, "data": ["Macaulay Carson Culkin", "New York", "United States", "August 26, 1980"]},
        { "idRow": 4, "data": ["Leonardo DiCaprio", "Los Angeles", "United States", "November 11, 1974"]},
        { "idRow": 5, "data": ["Tom Hanks", "Concord", "United States", "July 9, 1956"]},
        { "idRow": 6, "data": ["Meryl Streep", "Summit", "United States", "June 22, 1949"]},
        { "idRow": 7, "data": ["Brad Pitt", "Shawnee", "United States", "December 18, 1963"]},
        { "idRow": 8, "data": ["Natalie Portman", "Jerusalem", "Israel", "June 9, 1981"]},
        { "idRow": 9, "data": ["Robert De Niro", "New York", "United States", "August 17, 1943"]},
        { "idRow": 10, "data": ["Angelina Jolie", "Los Angeles", "United States", "June 4, 1975"]},
        { "idRow": 11, "data": ["Steven Spielberg", "Los Angeles", "United States", "December 18, 1946"]},
        { "idRow": 12, "data": ["Martin Scorsese", "New York", "United States", "November 17, 1942"]},
        { "idRow": 13, "data": ["Quentin Tarantino", "Knoxville", "United States", "March 27, 1963"]},
        { "idRow": 14, "data": ["Alfred Hitchcock", "London", "United Kingdom", "August 13, 1899"]},
        { "idRow": 15, "data": ["Stanley Kubrick", "New York", "United States", "July 26, 1928"]},
        { "idRow": 16, "data": ["Christopher Nolan", "London", "United Kingdom", "July 30, 1970"]},
        { "idRow": 17, "data": ["Francis Ford Coppola", "Detroit", "United States", "April 7, 1939"]},
        { "idRow": 18, "data": ["James Cameron", "Kapuskasing", "Canada", "August 16, 1954"]},
        { "idRow": 19, "data": ["Ridley Scott", "South Shields", "United Kingdom", "November 30, 1937"]},
        { "idRow": 20, "data": ["Woody Allen", "New York", "United States", "December 1, 1935"]}
    ]),
    "semanticAnnotations": fields.Nested(semantic_annotation_fields, example={ "cea": [], "cta": [], "cpa": []}),
    "metadata": fields.Nested(metadata, example={
        "column": [
            {"idColumn": 0, "tag": "NE"},
            {"idColumn": 1, "tag": "NE"},
            {"idColumn": 2, "tag": "NE"},
            {"idColumn": 3, "tag": "LIT", "datatype": "DATETIME"}
        ]
    }),
    "kgReference": fields.String(required=True, example="wikidata")
})


# Path to GeoLite2 database
db_path = './GeoLite2-City.mmdb'

# Check if the GeoLite2 database exists
if not os.path.exists(db_path):
    app.logger.error("GeoLite2 database file does not exist.")
    # Here, you could exit the application or disable geolocation features.
    # For this example, let's assume we disable it and continue running.
    geolocation_enabled = False
else:
    reader = geoip2.database.Reader(db_path)  # Load the database
    geolocation_enabled = True


def log_ip_and_increment_count():
    token = request.args.get('token')
    if token and validate_token(token, bypass=True):
        print("Bypassing rate limiting for special token", flush=True)
        return None  # Bypass rate limiting for special token

    ip_address = get_remote_address()
    today_date = datetime.date.today()
    
    geolocation = {}
    if geolocation_enabled:
        try:
            response = reader.city(ip_address)
            geolocation = {
                "city": response.city.name,
                "region": response.subdivisions.most_specific.name,
                "country": response.country.name,
                "latitude": response.location.latitude,
                "longitude": response.location.longitude
            }
        except geoip2.errors.AddressNotFoundError:
            geolocation = {"error": "IP address not found in database"}

    # Update or insert a new document with the current date and IP address
    ip_logs_c.update_one(
        {"ip_address": ip_address, "date": str(today_date)},
        {"$inc": {"count": 1}, "$set": {"geolocation": geolocation}},
        upsert=True
    )
    return ip_address


# Initialize Flask-Limiter
limiter = Limiter(
    app=app,
    key_func=log_ip_and_increment_count,  # Use custom function to retrieve IP and log it
    default_limits=["1000 per day"]
)



# Define a new route '/createWithArray' to handle batch creation of resources
@ds.route("/createWithArray")
@ds.doc(
    responses={
        202: "Accepted - The request has been accepted for processing.",
        400: "Bad Request - There was an error in the request. This might be due to invalid parameters or file format.",
        403: "Forbidden - Access denied due to invalid token."
    },
    params={ 
        "token": "token api key"
    }
)
class CreateWithArray(Resource):
    @ds.doc(
        body = [table_list_field],
        description="""
                        Upload a list of tables to annotate.
                    """
    )
    def post(self):
        """
            Receives an array of table data for bulk processing.
            This endpoint is used for annotating multiple tables in a single API call.
        """
        parser = reqparse.RequestParser()
        parser.add_argument("token", type=str, help="variable 1", location="args")
        args = parser.parse_args()
        token = args["token"]
        if not validate_token(token) and not validate_token(token, bypass=True):
            return {"Error": "Invalid Token"}, 403
        
        out = []

        try:
            tables = request.get_json()
            out = [{"datasetName": table["datasetName"], "tableName": table["tableName"]} for table in tables]
        except:
            print({"traceback": traceback.format_exc()}, flush=True)
            return {"Error": "Invalid Json"}, 400
        
        
        try:
            table = TableModel(mongoDBWrapper)
            table.parse_json(tables)
            table.store_tables()
            dataset = DatasetModel(mongoDBWrapper, table.table_metadata)
            dataset.store_datasets()
            tables = table.get_data()
            mongoDBWrapper.get_collection("row").insert_many(tables)
            job_active.delete("STOP")
            out = [{"id": str(table["_id"]), "datasetName": table["datasetName"], "tableName": table["tableName"]} for table in tables]
        except Exception as e:
            print({"traceback": traceback.format_exc()}, flush=True)
            #return {"status": "Error", "message": str(e)}, 400

        return {"status": "Ok", "tables": out}, 202
   

@ds.route("")
@ds.doc(
    responses={
        200: "Success: The requested data was found and returned.",
        404: "Not Found: The requested resource was not found on the server.",
        400: "Bad Request: The request was invalid or cannot be served.",
        403: "Forbidden: Invalid token or lack of access rights to the requested resource."
    },
    params={
        "token": {
            "description": "An API token for authentication and authorization purposes.",
            "type": "string",
            "required": True
        }
    },
    description="Operations related to datasets."
)
class Dataset(Resource):
    @ds.doc(
        params={
            "page": {
                "description": "The page number for paginated results. If not specified, defaults to 1.",
                "type": "int",
                "default": 1
            }
        },
        description="Retrieve datasets with pagination. Each page contains a subset of datasets."
    )
    def get(self):
        """
            Retrieves a paginated list of datasets. It uses the 'page' parameter to return the corresponding subset of datasets.
            Requires a valid token for authentication.

            Parameters:
            - token (str): An API token provided in the query string for access authorization.
            - page (int, optional): The page number for pagination, defaults to 1 if not specified.

            Returns:
            - A list of dataset summaries with their status and processing information, or an error message with an HTTP status code.
        """
        parser = reqparse.RequestParser()
        parser.add_argument("token", type=str, help="variable 1", location="args")
        parser.add_argument("page", type=str, help="variable 2", location="args")
        args = parser.parse_args()
        token = args["token"]

        
        if not validate_token(token) and not validate_token(token, bypass=True):
            return {"Error": "Invalid Token"}, 403
    
        page = args["page"]
        if page is None:
            page = 1
        elif page.isnumeric():
            page = int(page)
        else:
            return {"Error": "Invalid Number of Page"}, 403        
       
        try:
            results = dataset_c.find({"page": page})
            out = [
                {
                    "datasetName": result["datasetName"],
                    "Ntables": result["Ntables"],
                    "blocks": result["status"],
                    "%": result["%"],
                    "process": result["process"]
                }
                for result in results
            ]
            return out
        except Exception as e:
            print({"traceback": traceback.format_exc()}, flush=True)
            return {"status": "Error", "message": str(e)}, 400

    
    @ds.doc(
        params={
            "datasetName": {
                "description": "The name of the dataset to be created.",
                "type": "string",
                "required": True
            }
        },
        description="Create a new dataset with the specified name."
    )
    def post(self):
        """
            Creates a new dataset entry with the given name. Validates the provided token and adds an entry to the database.

            Parameters:
            - token (str): An API token provided in the query string for access authorization.
            - datasetName (str): The name of the new dataset to be created.

            Returns:
            - A confirmation message with the status of the dataset creation, or an error message with an HTTP status code.
        """
        parser = reqparse.RequestParser()
        parser.add_argument("token", type=str, help="variable 1", location="args")
        parser.add_argument("datasetName", type=str, help="variable 2", location="args")
        args = parser.parse_args()
        token = args["token"]
        dataset_name = args["datasetName"]

        if not validate_token(token) and not validate_token(token, bypass=True):
            return {"Error": "Invalid Token"}, 403
        
        data = {
            "datasetName": dataset_name,
            "Ntables": 0,
            "blocks": 0,
            "%": 0,
            "process": None
        }    
        try:
            result = {"message": f"Created dataset {dataset_name}"}, 200
            dataset_c.insert_one(data)
        except Exception as e:
            result = {"message": f"Dataset {dataset_name} already exist"}, 400

        return result


   
@ds.route("/<datasetName>")
@ds.doc(
    description="Retrieve data for a specific dataset. Allows pagination through the 'page' parameter.",
    responses={
        200: "OK - Returns a list of data related to the requested dataset.",
        404: "Not Found - The specified dataset could not be found.",
        400: "Bad Request - The request was invalid. This can be caused by missing or invalid parameters.",
        403: "Forbidden - Access denied due to invalid token."
    },
    params={ 
        "datasetName": {"description": "The name of the dataset to retrieve.", "type": "string"},
        "token": {"description": "API key token for authentication.", "type": "string"}
    }
)
class DatasetID(Resource):
    def get(self, datasetName):
        """
            Retrieves dataset information based on the dataset name and page number.
            Parameters:
                datasetName (str): The name of the dataset to be retrieved.
                token (str): API token for authentication.
            Returns:
                List[Dict]: A list of dictionaries containing dataset information.
                If an error occurs, returns a status message with the error detail.
        """
        parser = reqparse.RequestParser()
        parser.add_argument("token", type=str, help="variable 1", location="args")
        args = parser.parse_args()
        token = args["token"]
        dataset_name = datasetName

        if not validate_token(token) and not validate_token(token, bypass=True):
            return {"Error": "Invalid Token"}, 403


        try:    
            results = dataset_c.find({"datasetName": dataset_name})
            out = [
                {
                    "datasetName": result["datasetName"],
                    "Ntables": result["Ntables"],
                    "%": result["%"],
                    "status": result["process"]
                }
                for result in results
            ]
            return out
        except Exception as e:
            print({"traceback": traceback.format_exc()}, flush=True)
            return {"status": "Error", "message": str(e)}, 400


    def delete(self, datasetName):
        """
            Deletes a specific dataset based on the dataset name.
            Parameters:
                datasetName (str): The name of the dataset to be deleted.
                token (str): API token for authentication.
            Returns:
                Dict: A status message indicating the result of the delete operation.
                If an error occurs, returns a status message with the error detail.
        """
        parser = reqparse.RequestParser()
        parser.add_argument("token", type=str, help="variable 1", location="args")
        dataset_name = datasetName
        args = parser.parse_args()
        token = args["token"]
        if not validate_token(token) and not validate_token(token, bypass=True):
            return {"Error": "Invalid Token"}, 403
        try:
            self._delete_dataset(dataset_name)
        except Exception as e:
            print({"traceback": traceback.format_exc()}, flush=True)
            return {"status": "Error", "message": str(e)}, 400
        return {"datasetName": datasetName, "deleted": True}, 200           

    def _delete_dataset(self, dataset_name):
        query = {"datasetName": dataset_name}
        dataset_c.delete_one(query)
        row_c.delete_many(query)
        table_c.delete_one(query)
        cea_c.delete_many(query)
        cta_c.delete_many(query)
        cpa_c.delete_many(query)
        candidate_scored_c.delete_many(query)


@ds.route("/<datasetName>/table")
@ds.doc(
    description="Endpoint for uploading and processing a table within a specified dataset.",
    responses={
        200: "Success: The requested data was found and returned.",
        202: "Accepted - The request has been accepted for processing.",
        404: "Not Found - The specified dataset could not be found.",
        400: "Bad Request - There was an error in the request. This might be due to invalid parameters or file format.",
        403: "Forbidden - Access denied due to invalid token."
    },
    params={ 
        "token": {"description": "API key token for authentication.", "type": "string"}
    }
)
class DatasetTable(Resource):
    @ds.expect(upload_parser)
    @ds.doc(
        params={ 
            "kgReference": {
                "description": "Source Knowledge Graph (KG) of reference for the annotation process. Choice is 'wikidata'",
                "type": "string",
                "enum": ["wikidata"]  # Replace with your actual KG references
            }
        }
    )
    def post(self, datasetName):
        """
            Handles the uploading and processing of a table for a specified dataset.
            Parameters:
                datasetName (str): The name of the dataset to which the table will be added.
                kgReference (str): Optional. The reference Knowledge Graph for annotation (e.g., 'wikidata').
                token (str): API token for authentication.
            Returns:
                Dict: A status message and a list of processed tables, or an error message in case of failure.
        """
        parser = reqparse.RequestParser()
        parser.add_argument("kgReference", type=str, help="variable 1", location="args")
        parser.add_argument("token", type=str, help="variable 2", location="args")
        args = parser.parse_args()
        kg_reference = "wikidata"
        if args["kgReference"] is not None:
            kg_reference = args["kgReference"]
        token = args["token"]
        if not validate_token(token) and not validate_token(token, bypass=True):
            return {"Error": "Invalid Token"}, 403
        

        try:
            args = upload_parser.parse_args()
            uploaded_file = args["file"]  # This is FileStorage instance
            dataset_name = datasetName
            table_name = uploaded_file.filename.split(".")[0]
            out = [{"datasetName": datasetName, "tableName": table_name}]
            table = TableModel(mongoDBWrapper)
            num_rows = table.parse_csv(uploaded_file, dataset_name, table_name, kg_reference)
            table.store_tables(num_rows)
            dataset = DatasetModel(mongoDBWrapper, table.table_metadata)
            dataset.store_datasets()
            tables = table.get_data()
            row_c.insert_many(tables)    
            job_active.delete("STOP")
            out = [{"id": str(table["_id"]),  "datasetName": table["datasetName"], "tableName": table["tableName"]} for table in tables]
        except pymongo.errors.DuplicateKeyError as e:
            pass
            #print({"traceback": traceback.format_exc()}, flush=True)       
        except Exception as e:
            return {"status": "Error", "message": str(e), "traceback": traceback.format_exc()}, 400
        
        return {"status": "Ok", "tables": out}, 202
    
    @ds.doc(
        params={ 
            "page": {"description": "Source Knowledge Graph (KG) of reference for the annotation process. Default is 'wikidata'.", "type": "string"}
        }
    )
    def get(self, datasetName, page=None):
        """
            Handles the retrieval of information about tables within a specified dataset.
            Parameters:
                datasetName (str): The name of the dataset for which table information is requested.
                page (int): Page number for paginated results (default is 1).
                token (str): API token for authentication.
            Returns:
                List: A list of tables with their information, or an error message in case of failure.
        """
        parser = reqparse.RequestParser()
        parser.add_argument("page", type=int, help="variable 1", location="args")
        parser.add_argument("token", type=str, help="variable 1", location="args")
        args = parser.parse_args()
        page = args["page"]
        token = args["token"]

        if not validate_token(token) and not validate_token(token, bypass=True):
            return {"Error": "Invalid Token"}, 403
        
        if page is None:
            page = 1

        try:
            query = {"datasetName": datasetName, "page": int(page)}
            results = row_c.find(query)
            out = []
            for result in results:
                out.append({
                    "datasetName": result["datasetName"],
                    "tableName": result["tableName"],
                    "nrows": result["Nrows"],
                    "status": result["status"]
                })
        except Exception as e:
            print({"traceback": traceback.format_exc()}, flush=True)
            out = {"status": "Error", "message": str(e)}, 400        

        return out, 200
    


@ds.route("/<datasetName>/table/<tableName>")
@ds.doc(
    description="Endpoint for retrieving and deleting specific tables within a dataset.",
    responses={
        200: "OK - Successfully retrieved or deleted the specified table.",
        404: "Not Found - The specified table or dataset could not be found.",
        400: "Bad Request - The request was invalid, possibly due to incorrect parameters.",
        403: "Forbidden - Access denied due to invalid token."
    },
    params={ 
        "token": {"description": "API key token for authentication.", "type": "string"}
    }
)
class TableID(Resource):
    @ds.doc(
        params={
            "page": {
                "description": "The page number for pagination of table data. Defaults to returning all pages if not specified.",
                "type": "int",
                "default": 1
            }
        },
        description="Retrieve results with pagination. Each page contains a subset of results."
    )
    def get(self, datasetName, tableName):
        """
            Retrieves a specific table from a dataset based on the dataset and table names.
            Parameters:
                datasetName (str): The name of the dataset.
                tableName (str): The name of the table to retrieve.
                page (int): Optional. The page number for pagination of table data.
                token (str): API token for authentication.
            Returns:
                Dict: A dictionary containing the requested table data, or an error message in case of failure.
        """
        parser = reqparse.RequestParser()
        parser.add_argument("page", type=int, help="variable 1", location="args")
        parser.add_argument("token", type=str, help="variable 1", location="args")
        args = parser.parse_args()
        page = args["page"]
        token = args["token"]
        
       
        if not validate_token(token) and not validate_token(token, bypass=True):
            return {"Error": "Invalid Token"}, 403

        # if page isn't specified, return all pages
        """
        if page is None:
            page = 1
        """
        # will have to change in the future 
        
        try:
            out = self._get_table(datasetName, tableName, page)
            out = self._replace_nan_with_none(out)  # Replace NaN with None in the output
            return out
        except Exception as e:
            print({"traceback": traceback.format_exc()}, flush=True)
            return {"status": "Error", "message": str(e)}, 404
    

    def _replace_nan_with_none(self, value):
        """
        Recursively replace NaN values with None in the given data structure.
        """
        if isinstance(value, float) and math.isnan(value):
            return None
        elif isinstance(value, dict):
            return {k: self._replace_nan_with_none(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._replace_nan_with_none(v) for v in value]
        return value
    
    def _get_table(self, dataset_name, table_name, page=None):
        query = {"datasetName": dataset_name, "tableName": table_name}
        if page is not None:
            query["page"] = page
        results = row_c.find(query)
        out = [
            {
                "datasetName": result["datasetName"],
                "tableName": result["tableName"],
                "header": result["header"],
                "rows": result["rows"],
                "semanticAnnotations": {"cea": [], "cpa": [], "cta": []},
                "metadata": result.get("metadata", []),
                "status": result["status"]
            }
            for result in results
        ]

        if len(out) == 0:
            return {"status": "Error", "message": "Table not found"}, 404
        
        buffer = out[0]
        for o in out[1:]:
            buffer["rows"] += o["rows"]
        buffer["nrows"] = len(buffer["rows"])

        if len(out) > 0:
            if page is None:
                out = buffer
            else:
                out = out[0]
            doing = True
            results = cea_c.find(query)
            total = cea_c.count_documents(query)
            if total == len(out["rows"]):
                doing = False
            for result in results:
                winning_candidates = result["winningCandidates"]
                for id_col, candidates in enumerate(winning_candidates):
                    entities = []
                    for candidate in candidates[0:3]:
                        entities.append({
                            "id": candidate["id"],
                            "name": candidate["name"],
                            "type": candidate["types"],
                            "description": candidate["description"],
                            "match": candidate["match"],
                            "score": candidate.get("rho'"),
                            "features": [
                                {"id":"delta", "value": candidate.get("delta")},
                                {"id":"omega", "value": candidate.get("score")},
                                {"id":"levenshtein_distance", "value": candidate["features"].get("ed_score")},
                                {"id":"jaccard_distance", "value": candidate["features"].get("jaccard_score")},
                                {"id":"popularity", "value": candidate["features"].get("popularity")}
                            ]
                        })
                    out["semanticAnnotations"]["cea"].append({
                        "idColumn": id_col,
                        "idRow": result["row"],
                        "entity": entities
                    })
            out["status"] = "DONE" if doing is False else "DOING"        
            result = cpa_c.find_one(query)
            if result is not None:
                winning_predicates = result["cpa"]
                for id_source_column in winning_predicates:
                    for id_target_column in winning_predicates[id_source_column]:
                        out["semanticAnnotations"]["cpa"].append({
                            "idSourceColumn": id_source_column,
                            "idTargetColumn": id_target_column,
                            "predicate": winning_predicates[id_source_column][id_target_column]
                        })

            result = cta_c.find_one(query)
            if result is not None:
                winning_types = result["cta"]
                for id_col in winning_types:
                    out["semanticAnnotations"]["cta"].append({
                        "idColumn": int(id_col),
                        "types": [winning_types[id_col]]
                    })            
        return out
    
    
    def delete(self, datasetName, tableName):
        """
            Deletes a specific table from a dataset based on the dataset and table names.
            Parameters:
                datasetName (str): The name of the dataset.
                tableName (str): The name of the table to be deleted.
                token (str): API token for authentication.
            Returns:
                Dict: A status message indicating the result of the delete operation.
        """
        parser = reqparse.RequestParser()
        parser.add_argument("token", type=str, help="variable 1", location="args")
        args = parser.parse_args()
        token = args["token"]
       
        if not validate_token(token) and not validate_token(token, bypass=True):
            return {"Error": "Invalid Token"}, 403

        try:
            self._delete_table(datasetName, tableName)
        except Exception as e:
            print({"traceback": traceback.format_exc()}, flush=True)
            return {"status": "Error", "message": str(e)}, 400

        return {"datasetName": datasetName, "tableName": tableName, "deleted": True}, 200
    
    def _delete_table(self, dataset_name, table_name):
        query = {"datasetName": dataset_name, "tableName": table_name}
        row_c.delete_many(query)
        table_c.delete_one(query)
        cea_prelinking_c.delete_many(query)
        cea_c.delete_many(query)
        cta_c.delete_many(query)
        cpa_c.delete_many(query)
        candidate_scored_c.delete_many(query)
       

if __name__ == "__main__":
    app.run(debug=True)
