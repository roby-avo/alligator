import os
import logging
import redis
import pymongo
import math
import traceback
import geoip2.database
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_restx import Api, Resource, fields, reqparse
from werkzeug.datastructures import FileStorage
from datetime import datetime, timedelta

# Disable TensorFlow warnings and suppress Python warnings
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
logging.getLogger('tensorflow').setLevel(logging.ERROR)

from process.wrapper.Database import MongoDBWrapper
from utils.Dataset import DatasetModel
from utils.Table import TableModel

# Configuration
REDIS_ENDPOINT = os.environ["REDIS_ENDPOINT"]
REDIS_JOB_DB = int(os.environ["REDIS_JOB_DB"])
API_TOKEN = os.environ["ALLIGATOR_TOKEN"]
UNLIMITED_TOKEN = os.environ["ALLIGATOR_TOKEN_SECRET"]
MAXIMUM_REQUESTS_PER_DAY = os.environ["MAXIMUM_REQUESTS_PER_DAY"]
MAX_CONTENT_LENGTH = 500 * 1024 * 1024  # 500MB limit
MAX_PER_PAGE = 100  # Define a sensible maximum limit for items per page



# Initialize Redis client and MongoDB wrapper
job_active = redis.Redis(host=REDIS_ENDPOINT, db=REDIS_JOB_DB)
mongoDBWrapper = MongoDBWrapper()
row_c = mongoDBWrapper.get_collection("row")
cea_prelinking_c = mongoDBWrapper.get_collection("ceaPrelinking")
candidate_scored_c = mongoDBWrapper.get_collection("candidateScored")
cea_c = mongoDBWrapper.get_collection("cea")
cpa_c = mongoDBWrapper.get_collection("cpa")
cta_c = mongoDBWrapper.get_collection("cta")
dataset_c = mongoDBWrapper.get_collection("dataset")
table_c = mongoDBWrapper.get_collection("table")
rate_limit_c = mongoDBWrapper.get_collection("rateLimit")

# Initialize Flask application and enable CORS
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH  # Set maximum request size to 500MB
CORS(app)

# Load API description
with open("data.txt") as f:
    description = f.read()

api = Api(app, version="1.0", title="Alligator", description=description)
ds = api.namespace("dataset", description="Dataset namespace")

upload_parser = api.parser()
upload_parser.add_argument("file", location="files", type=FileStorage, required=True)

# Token validation function
def validate_token(token):
    return token == API_TOKEN 


# GeoIP database setup
db_path = './GeoLite2-City.mmdb'
geolocation_enabled = os.path.exists(db_path)
if geolocation_enabled:
    reader = geoip2.database.Reader(db_path)
else:
    app.logger.error("GeoLite2 database file does not exist.")


# Rate limiting and geolocation tracking based on IP address
@app.before_request
def before_request():
    # List of paths to exempt from validation
    swagger_endpoints = [
        '/', 
        '/swagger.json', 
        '/swaggerui/droid-sans.css', 
        '/swaggerui/swagger-ui-bundle.js', 
        '/swaggerui/swagger-ui-standalone-preset.js', 
        '/swaggerui/swagger-ui.css', 
        '/favicon-32x32.png', 
        '/favicon-16x16.png',
        '/swaggerui/favicon-32x32.png',  # Including exact path for favicons
        '/swaggerui/favicon-16x16.png'
    ]
    
    # Token validation
    token = request.args.get('token')

    # Exempting Swagger UI paths from validation
    if request.path in swagger_endpoints or token == UNLIMITED_TOKEN:
        return
        
    if not validate_token(token):
        return jsonify({"Error": "Invalid Token"}), 403

    # Rate limiting based on IP address
    ip_address = request.headers.get('X-Forwarded-For', request.remote_addr).split(',')[0]
    rate_limit_key = ip_address
    
    now = datetime.now()
    limit_window = timedelta(days=1)
    max_requests = 1000  # Maximum number of requests per day

    rate_limit_record = rate_limit_c.find_one({"ip": rate_limit_key, "date": str(now.date())})

    if rate_limit_record:
        last_request_time = rate_limit_record["last_request"]
        request_count = rate_limit_record["count"]

        if now - last_request_time < limit_window:
            if request_count >= max_requests:
                return jsonify({"Error": "Rate limit exceeded"}), 429
            else:
                rate_limit_c.update_one(
                    {"ip": rate_limit_key, "date": str(now.date())},
                    {"$inc": {"count": 1}, "$set": {"last_request": now}}
                )
        else:
            rate_limit_c.update_one(
                {"ip": rate_limit_key, "date": str(now.date())},
                {"$set": {"count": 1, "last_request": now}}
            )
    else:
        rate_limit_c.insert_one({"ip": rate_limit_key, "count": 1, "last_request": now, "date": str(now.date())})

    # Geolocation tracking
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

        # Log IP and geolocation information
        rate_limit_c.update_one(
            {"ip": rate_limit_key, "date": str(now.date())},
            {"$set": {"geolocation": geolocation}},
            upsert=True
        )


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



# Define a new route '/createWithArray' to handle batch creation of resources
@ds.route("/createWithArray")
@ds.doc(
    responses={
        202: "Accepted - The request has been accepted for processing.",
        400: "Bad Request - There was an error in the request. This might be due to invalid parameters or file format.",
        403: "Forbidden - Access denied due to invalid token.",
        413: "Payload Too Large - The request payload exceeds the maximum size limit.",
        429: "Too Many Requests - The rate limit for the API has been exceeded."
    },
    params={ 
        "token": {
            "description": "An API token for authentication and authorization purposes.",
            "type": "string",
            "required": True
        }
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
            },
            "per_page": {
                "description": "The number of items per page. If not specified, defaults to 10.",
                "type": "int",
                "default": 10
            }
        },
        description="Create a new dataset with the specified name."
    )
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument("token", type=str, help="API token for access", location="args")
        parser.add_argument("page", type=int, help="Page number for pagination", location="args", default=1)
        parser.add_argument("per_page", type=int, help="Items per page for pagination", location="args", default=10)
        args = parser.parse_args()
        token = args["token"]
        page = args["page"]
        per_page = args["per_page"]
        
        try:
            page = max(1, int(page))
            per_page = max(1, min(int(per_page), MAX_PER_PAGE))  # Enforce maximum limit for per_page
            skip = (page - 1) * per_page
            total_items = dataset_c.count_documents({})
            total_pages = math.ceil(total_items / per_page)

            if page > total_pages:
                return {
                    "data": [],
                    "pagination": {
                        "currentPage": page,
                        "perPage": per_page,
                        "totalPages": total_pages,
                        "totalItems": total_items
                    },
                    "message": "Page number exceeds total number of pages."
                }

            results = dataset_c.find().skip(skip).limit(per_page)
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

            return {
                "data": out,
                "pagination": {
                    "currentPage": page,
                    "perPage": per_page,
                    "totalPages": total_pages,
                    "totalItems": total_items
                }
            }
        except Exception as e:
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

        try:
            dataset = DatasetModel(mongoDBWrapper, {dataset_name: {}})
            dataset.store_datasets()
            result = {"message": f"Created dataset {dataset_name}"}, 200
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
        403: "Forbidden - Access denied due to invalid token.",
        413: "Payload Too Large - The request payload exceeds the maximum size limit.",
        429: "Too Many Requests - The rate limit for the API has been exceeded."
    },
    params={ 
        "datasetName": {"description": "The name of the dataset to retrieve.", "type": "string"},
        "token": {
            "description": "An API token for authentication and authorization purposes.",
            "type": "string",
            "required": True
        }
    }
)
class DatasetID(Resource):
    def get(self, datasetName):
        """
            Retrieves dataset information based on the dataset name and page number.
            Parameters:
                datasetName (str): The name of the dataset to be retrieved.
                page (int): Optional. The page number for pagination of results.
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

        try:    
            results = dataset_c.find({"datasetName": dataset_name})
            out = [
                {
                    "datasetName": result["datasetName"],
                    "Ntables": result["Ntables"],
                    "%": result["%"],
                    "status": result["process"],
                    "page": result["page"]
                }
                for result in results
            ]
            return out
        except Exception as e:
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
       
        try:
            self._delete_dataset(dataset_name)
            return {"datasetName": datasetName, "deleted": True}, 200     
        except Exception as e:
            print({"traceback": traceback.format_exc()}, flush=True)
            return {"status": "Error", "message": str(e)}, 400
              

    def _delete_dataset(self, dataset_name):
        query = {"datasetName": dataset_name}
        dataset_c.delete_one(query)
        row_c.delete_many(query)
        table_c.delete_many(query)
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
        403: "Forbidden - Access denied due to invalid token.",
        413: "Payload Too Large - The request payload exceeds the maximum size limit.",
        429: "Too Many Requests - The rate limit for the API has been exceeded."
    },
    params={ 
        "token": {
            "description": "An API token for authentication and authorization purposes.",
            "type": "string",
            "required": True
        }
    }
)
class DatasetTable(Resource):
    @ds.expect(upload_parser)
    @ds.doc(
        params={ 
            "kgReference": {"description": "Source Knowledge Graph (KG) of reference for the annotation process. Default is 'wikidata'.", "type": "string"}
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
            return {"status": "Ok", "tables": out}, 202
        except pymongo.errors.DuplicateKeyError as e:
            pass
            #print({"traceback": traceback.format_exc()}, flush=True)       
        except Exception as e:
            return {"status": "Error", "message": str(e), "traceback": traceback.format_exc()}, 400
        
        
    @ds.doc(
        params={
            "page": {
                "description": "The page number for paginated results, default is 1.",
                "type": "int",
                "default": 1
            },
            "per_page": {
                "description": "The number of items per page, default is 10.",
                "type": "int",
                "default": 10
            }
        },
        description="Retrieve tables within dataset with pagination. Each page contains a subset of tables."
    )
    def get(self, datasetName):
        """
            Handles the retrieval of information about tables within a specified dataset.
            Parameters:
                datasetName (str): The name of the dataset for which table information is requested.
                page (int): Page number for paginated results (default is 1).
                per_page (int): Number of items per page (default is 10).
                token (str): API token for authentication.
            Returns:
                List: A list of tables with their information, or an error message in case of failure.
        """
        parser = reqparse.RequestParser()
        parser.add_argument("page", type=int, help="Page number for pagination", location="args", default=1)
        parser.add_argument("per_page", type=int, help="Items per page for pagination", location="args", default=10)
        parser.add_argument("token", type=str, help="API token for access", location="args")
        args = parser.parse_args()
        page = args["page"]
        per_page = args["per_page"]
        token = args["token"]
        
        try:
            page = max(1, int(page))
            per_page = max(1, min(int(per_page), MAX_PER_PAGE))  # Enforce maximum limit for per_page
            skip = (page - 1) * per_page
            total_items = table_c.count_documents({"datasetName": datasetName})
            total_pages = math.ceil(total_items / per_page)
            
            if page > total_pages:
                return {
                    "data": [],
                    "pagination": {
                        "currentPage": page,
                        "perPage": per_page,
                        "totalPages": total_pages,
                        "totalItems": total_items
                    },
                    "message": "Page number exceeds total number of pages."
                }, 200

            query = {"datasetName": datasetName}
            results = table_c.find(query).skip(skip).limit(per_page)
            out = []
            for result in results:
                out.append({
                    "datasetName": result["datasetName"],
                    "tableName": result["tableName"],
                    "nrows": result["Nrows"],
                    "status": result["status"]
                })

            return {
                "data": out,
                "pagination": {
                    "currentPage": page,
                    "perPage": per_page,
                    "totalPages": total_pages,
                    "totalItems": total_items
                }
            }, 200
        except Exception as e:
            print({"traceback": traceback.format_exc()}, flush=True)
            return {"status": "Error", "message": str(e)}, 400  

       
@ds.route("/<datasetName>/table/<tableName>")
@ds.doc(
    description="Endpoint for retrieving and deleting specific tables within a dataset.",
    responses={
        200: "OK - Successfully retrieved or deleted the specified table.",
        404: "Not Found - The specified table or dataset could not be found.",
        400: "Bad Request - The request was invalid, possibly due to incorrect parameters.",
        403: "Forbidden - Access denied due to invalid token.",
        413: "Payload Too Large - The request payload exceeds the maximum size limit.",
        429: "Too Many Requests - The rate limit for the API has been exceeded."
    },
    params={ 
        "token": {
            "description": "An API token for authentication and authorization purposes.",
            "type": "string",
            "required": True
        }
    }
)
class TableID(Resource):
    @ds.doc(
        params={
            "page": {
                "description": "The page number for paginated results, default is 1.",
                "type": "int",
                "default": 1
            },
            "per_page": {
                "description": "The number of items per page, default is 10.",
                "type": "int",
                "default": 10
            },
            "column": {
                "description": "The column number which to sort the table data by.",
                "type": "int",
                "default": None
            },
            "sort": {
                "description": "The sorting order for the table data.",
                "type": "string",
                "default": None
            }
        },
        description="Retrieve tables within dataset with pagination. Each page contains a subset of tables."
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
        parser.add_argument("page", type=int, help="Page number for pagination", location="args", default=1)
        parser.add_argument("per_page", type=int, help="Items per page for pagination", location="args", default=10)
        parser.add_argument("column", type=int, help="Column number for sorting", location="args", default=None)
        parser.add_argument("sort", type=str, help="Sorting order for table data", location="args", default=None)
        parser.add_argument("token", type=str, help="variable 1", location="args")
        args = parser.parse_args()
        page = args["page"]
        per_page = args["per_page"]
        column = args["column"]
        sort = args["sort"]
        token = args["token"]
        
        query = {"datasetName": datasetName, "tableName": tableName}
        page = max(1, int(page))
        per_page = max(1, min(int(per_page), MAX_PER_PAGE))  # Enforce maximum limit for per_page
        skip = (page - 1) * per_page

        if column is not None and sort is not None:
            # Define query for sorting by column
            new_query = {
                'datasetName': datasetName,
                'tableName': tableName,
                '$expr': {
                    '$gt': [
                        {'$size': {'$arrayElemAt': ['$winningCandidates', column]}},
                        0
                    ]
                }
            }
            total_items = cea_c.count_documents(new_query)
        else:    
            total_items = cea_c.count_documents(query)
       
        total_pages = math.ceil(total_items / per_page)
        is_cea_available = True

        if total_items == 0:
            per_page = 1
            skip = (page - 1) * per_page
            total_pages = row_c.count_documents(query)
            total_items = total_pages
            is_cea_available = False

        try:
            out = self._get_table(query, skip, per_page, is_cea_available, column, sort)
            out = self._replace_nan_with_none(out)  # Replace NaN with None in the output
            return {
                "data": out,
                "pagination": {
                    "currentPage": page,
                    "perPage": per_page,
                    "totalPages": total_pages,
                    "totalItems": total_items
                }
            }, 200
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
    
    def _get_table(self, query, skip, per_page, is_cea_available=False, column=None, sort=None):
        status = table_c.find_one(query).get("status")
        if not is_cea_available:
            print(skip, per_page, query, flush=True)
            result = row_c.find(query).skip(skip).limit(per_page)
            result = list(result)
            if len(result) > 0:
                result = result[0]
            out = {
                "datasetName": result["datasetName"],
                "tableName": result["tableName"],
                "header": result["header"],
                "rows": result["rows"],
                "semanticAnnotations": {"cea": [], "cpa": [], "cta": []},
                "metadata": result.get("metadata", []),
                "status": status
            }
            return out
        else:
            result = row_c.find_one(query)
            object = {
                "datasetName": result["datasetName"],
                "tableName": result["tableName"],
                "header": result["header"],
                "rows": [],
                "semanticAnnotations": {"cea": [], "cpa": [], "cta": []},
                "metadata": result.get("metadata", []),
                "status": status
            }

            print("Sorting by column", column, "in", sort, "order", flush=True)
            if column is not None and sort is not None:
                print("Sorting by column", column, "in", sort, "order", flush=True)
                results = self._get_annotations_by_confidence(query, skip, per_page, column, sort)
            else:
                results = cea_c.find(query).skip(skip).limit(per_page)

            for result in results:
                object["rows"].append({
                    "idRow": result["row"],
                    "data": result["data"]
                })
                winning_candidates = result["winningCandidates"]
                for id_col, candidates in enumerate(winning_candidates):
                    entities = []
                    for candidate in candidates:
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
                    object["semanticAnnotations"]["cea"].append({
                        "idColumn": id_col,
                        "idRow": result["row"],
                        "entity": entities
                    })
                
            result = cpa_c.find_one(query)
            if result is not None:
                winning_predicates = result["cpa"]
                for id_source_column in winning_predicates:
                    for id_target_column in winning_predicates[id_source_column]:
                        object["semanticAnnotations"]["cpa"].append({
                            "idSourceColumn": id_source_column,
                            "idTargetColumn": id_target_column,
                            "predicate": winning_predicates[id_source_column][id_target_column]
                        })

            result = cta_c.find_one(query)
            if result is not None:
                winning_types = result["cta"]
                for id_col in winning_types:
                    object["semanticAnnotations"]["cta"].append({
                        "idColumn": int(id_col),
                        "types": [winning_types[id_col]]
                    })            
            return object
    

    def _get_annotations_by_confidence(self, query, skip, per_page, column, sort):
        sort_type = pymongo.DESCENDING if sort == "desc" else pymongo.ASCENDING
        # Run the aggregation query with pagination
        pipeline = [
            { 
                '$match': { 
                    'datasetName': query["datasetName"], 
                    'tableName': query["tableName"], 
                    '$expr': { 
                        '$gt': [{ '$size': { '$arrayElemAt': ['$winningCandidates', 0] } }, 0] 
                    } 
                } 
            },
            { 
                '$sort': { 
                    f"winningCandidates.{column}.0.rho'": sort_type
                } 
            },
            { 
                '$skip': skip
            },
            { 
                '$limit': per_page
            }
        ]
        print("Aggregation pipeline:", pipeline, flush=True)
        results = cea_c.aggregate(pipeline)
        return results
    
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
       

        try:
            self._delete_table(datasetName, tableName)
            return {"datasetName": datasetName, "tableName": tableName, "deleted": True}, 200
        except Exception as e:
            print({"traceback": traceback.format_exc()}, flush=True)
            return {"status": "Error", "message": str(e)}, 400

    
    def _delete_table(self, dataset_name, table_name):
        query = {"datasetName": dataset_name, "tableName": table_name}
        row_c.delete_many(query)
        table_c.delete_one(query)
        cea_c.delete_many(query)
        cta_c.delete_many(query)
        cpa_c.delete_many(query)
        candidate_scored_c.delete_many(query)
       

if __name__ == "__main__":
    app.run(debug=True)
