
import os
import traceback
import pandas as pd
import redis
from flask import Flask, request
from flask_cors import CORS
from flask_restx import Api, Resource, fields, reqparse
from werkzeug.datastructures import FileStorage

import utils.api_utils as utils
from indexing import index
from process.wrapper.Database import MongoDBWrapper
from utils.Dataset import DatasetModel
from utils.Table import TableModel

index.create_index()

REDIS_ENDPOINT = os.environ["REDIS_ENDPOINT"]
REDIS_JOB_DB = int(os.environ["REDIS_JOB_DB"])

API_TOKEN = os.environ["API_TOKEN"]

job_active = redis.Redis(host=REDIS_ENDPOINT, db=REDIS_JOB_DB)

mongoDBWrapper = MongoDBWrapper()
row_c = mongoDBWrapper.get_collection("row")
candidate_scored_c = mongoDBWrapper.get_collection("candidateScored")
cea_c = mongoDBWrapper.get_collection("cea")
cpa_c = mongoDBWrapper.get_collection("cpa")
cta_c = mongoDBWrapper.get_collection("cta")
dataset_c = mongoDBWrapper.get_collection("dataset")
table_c = mongoDBWrapper.get_collection("table")

app = Flask(__name__)
CORS(app)
with open("data.txt") as f:
    description = f.read()

api = Api(app, version="1.0", title="Alligator", description=description)
ds = api.namespace("dataset", description="Dataset namespace")


upload_parser = api.parser()
upload_parser.add_argument("file", location="files",
                           type=FileStorage, required=True)


def validate_token(token):
    return token == API_TOKEN


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
    "header": fields.List(fields.String(), required=True, example=["col1", "col2", "col3"]),
    "rows": fields.List(fields.String(), required=True, example= [
        { "idRow": 1, "data": ["Zooey Deschanel", "Los Angeles", "United States"]},
        { "idRow": 2, "data": ["Sarah Mclachlan", "Halifax", "Canada"]},
        { "idRow": 3, "data": ["Macaulay Carson Culkin", "New York", "United States"]}
    ]),
    "semanticAnnotations": fields.Nested(semantic_annotation_fields, example={ "cea": [], "cta": [], "cpa": []}),
    "metadata": fields.Nested(metadata, example={
        "column": [
            {"idColumn": 0, "tag": "NE"},
            {"idColumn": 1, "tag": "NE"},
            {"idColumn": 2, "tag": "NE"}
        ]
    }),
    "kgReference": fields.String(required=True, example="wikidata")
})


@ds.route("/createWithArray")
@ds.doc(
    responses={200: "OK", 404: "Not found",
               400: "Bad request", 403: "Invalid token"},
    params={ 
        "token": "token api key"
    }
)
class CreateWithArray(Resource):
    @ds.doc(
        body = [table_list_field],
        description="""
                     Upload a sample of table to annotate
                    """
    )
    def post(self):
        """
            Upload table data using array
        """
        parser = reqparse.RequestParser()
        parser.add_argument("token", type=str, help="variable 1", location="args")
        args = parser.parse_args()
        token = args["token"]
        if not validate_token(token):
            return {"Error": "Invalid Token"}, 403
        
        out = []

        try:
            tables = request.get_json()
            for table in tables:
                dataset_name = table["datasetName"]
                table_name = table["tableName"]
                response = request.get(f"http://localhost:5000/dataset/{dataset_name}/table/{table_name}?page=1&token={token}")
                response = response.json()
                if response["status"] == "DOING":
                    out.append({"datasetName": dataset_name, "tableName": table_name})
        except:
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
            #out = [{"id": str(table["_id"]), "datasetName": table["datasetName"], "tableName": table["tableName"]} for table in tables]
        except Exception as e:
            print({"traceback": traceback.format_exc()}, flush=True)
            #return {"status": "Error", "message": str(e)}, 400

        return {"status": "Ok", "tables": out}, 200
   

@ds.route("")
@ds.doc(
    responses={200: "OK", 404: "Not found",
               400: "Bad request", 403: "Invalid token"},
    params={ 
        "token": {"description": "token key api",
                                "type": "string", "required": True}
    },
    description="""
                """
)
class Dataset(Resource):
    @ds.doc(params={"page": {"description": "Number of page",
                                "type": "int", "default": 1}})
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument("token", type=str, help="variable 1", location="args")
        parser.add_argument("page", type=str, help="variable 2", location="args")
        args = parser.parse_args()
        token = args["token"]
        page = args["page"]
        if page is None:
            page = 1
        elif page.isnumeric():
            page = int(page)
        else:
            return {"Error": "Invalid Number of Page"}, 403        
        if not validate_token(token):
            return {"Error": "Invalid Token"}, 403
        
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

    
    @ds.doc(params={"datasetName": {"description": "Created dataset",
                                "type": "string", "required": True}})
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument("token", type=str, help="variable 1", location="args")
        parser.add_argument("datasetName", type=str, help="variable 2", location="args")
        args = parser.parse_args()
        token = args["token"]
        dataset_name = args["datasetName"]
        if not validate_token(token):
            return {"Error": "Invalid Token"}, 403
        data = {
            "datasetName": dataset_name,
            "Ntables": 0,
            "blocks": 0,
            "%": 0,
            "process": None
        }    
        try:
            dataset_c.insert_one(data)
            result = {"message": f"Created dataset {dataset_name}"}, 200
        except:
            result = {"message": f"Dataset {dataset_name} already exist"}, 400
        return result


   
@ds.route("/<datasetName>")
@ds.doc(
    responses={200: "OK", 404: "Not found",
               400: "Bad request", 403: "Invalid token"},
    params={ 
        "datasetName": "The name of dataset",
        "page": "page number",
        "token": "token api key"
    }
)
class DatasetID(Resource):
    def get(self, datasetName):
        #results = row_c.aggregate([{"$match":{"dataset":dataset}}, {"$group": {"_id":{"name":"$name"}}}])
        parser = reqparse.RequestParser()
        parser.add_argument("token", type=str, help="variable 1", location="args")
        parser.add_argument("page", type=int, help="variable 2", location="args")
        args = parser.parse_args()
        token = args["token"]
        dataset_name = datasetName
        page = args["page"]
        if page is None or len(page) == 0:
            page = 1
        
        try:    
            results = table_c.find({"datasetName": dataset_name, "page": page})
            out = [
                {
                    "tableName": result["tableName"],
                    "status": result["process"]
                }
                for result in results
            ]
            return out
        except Exception as e:
            print({"traceback": traceback.format_exc()}, flush=True)
            return {"status": "Error", "message": str(e)}, 400


    def delete(self, datasetName):
        parser = reqparse.RequestParser()
        parser.add_argument("token", type=str, help="variable 1", location="args")
        parser.add_argument("datasetName", type=str, help="variable 2", location="args")
        args = parser.parse_args()
        token = args["token"]
        dataset_name = args["datasetName"]
        if not validate_token(token):
            return {"Error": "Invalid Token"}, 403
        try:
            result = dataset_c.delete_one({"datasetName": dataset_name})
        except Exception as e:
            print({"traceback": traceback.format_exc()}, flush=True)
            return {"status": "Error", "message": str(e)}, 400
        return list(result), 200           



@ds.route("/<datasetName>/table")
@ds.expect(upload_parser)
@ds.doc(
    responses={200: "OK", 404: "Not found",
               400: "Bad request", 403: "Invalid token"},
    params={ 
        "kgReference": "source KG of reference for the annotation process",
        "token": "token api key"
    }
)
class Upload(Resource):
    def post(self, datasetName):
        parser = reqparse.RequestParser()
        parser.add_argument("kgReference", type=str, help="variable 1", location="args")
        parser.add_argument("token", type=str, help="variable 2", location="args")
        args = parser.parse_args()
        kg_reference = "wikidata"
        if args["kgReference"] is not None:
            kg_reference = args["kgReference"]
        token = args["token"]
        if not validate_token(token):
            return {"Error": "Invalid Token"}, 403
        
        try:
            args = upload_parser.parse_args()
            uploaded_file = args["file"]  # This is FileStorage instance
            dataset_name = datasetName
            table_name = uploaded_file.filename.split(".")[0]
            table = TableModel(mongoDBWrapper)
            table.parse_csv(uploaded_file, dataset_name, table_name, kg_reference)
            table.store_tables()
            dataset = DatasetModel(mongoDBWrapper, table.table_metadata)
            dataset.store_datasets()
            tables = table.get_data()
            mongoDBWrapper.get_collection("row").insert_many(tables)    
            job_active.delete("STOP")
            out = [{"id": str(table["_id"]),  "datasetName": table["datasetName"], "tableName": table["tableName"]} for table in tables]
        except Exception as e:
            return {"status": "Error", "message": str(e), "traceback": traceback.format_exc()}, 400
        
        return {"status": "Ok", "tables": out}, 200


@ds.route("/<datasetName>/table/<tableName>")
@ds.doc(
    responses={200: "OK", 404: "Not found",
               400: "Bad request", 403: "Invalid token"},
    params={ 
        "page": " The page number of the results. It starts from 1. If not specified, it will return all pages",
        "token": " Your API key token for authentication"
    }
)
class TableID(Resource):
    def get(self, datasetName, tableName):
        parser = reqparse.RequestParser()
        parser.add_argument("page", type=int, help="variable 1", location="args")
        parser.add_argument("token", type=str, help="variable 1", location="args")
        args = parser.parse_args()
        page = args["page"]
        token = args["token"]
        #stringId = args["stringId"] == "true"
       
        if not validate_token(token):
            return {"Error": "Invalid Token"}, 403

        # if page isn't specified, return all pages
        """
        if page is None:
            page = 1
        """
        # will have to change in the future 
        
        if page is None:
            query = {"datasetName": datasetName, "tableName": tableName}
        else:    
            query = {"datasetName": datasetName, "tableName": tableName, "page": page}
    
        try:
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
                                "delta": candidate["delta"],
                                "score": candidate["score"]
                            })
                        out["semanticAnnotations"]["cea"].append({
                            "idColumn": id_col,
                            "idRow": result["row"],
                            "entity": entities
                        })
                out["status"] = "DONE" if doing is False else "DOING"        
                results = cpa_c.find(query)
                for result in results:
                    winning_predicates = result["cpa"]
                    for id_source_column in winning_predicates:
                        for id_target_column in winning_predicates[id_source_column]:
                            out["semanticAnnotations"]["cpa"].append({
                                "idSourceColumn": id_source_column,
                                "idTargetColumn": id_target_column,
                                "predicate": winning_predicates[id_source_column][id_target_column]
                            })
                results = cta_c.find(query)
                for result in results:
                    winning_types = result["cta"]
                    for id_col in winning_types:
                        out["semanticAnnotations"]["cta"].append({
                            "idColumn": int(id_col),
                            "types": [winning_types[id_col]]
                        })        
            return out
        except Exception as e:
            print({"traceback": traceback.format_exc()}, flush=True)
            return {"status": "Error", "message": str(e)}, 400
    
        
    def delete(self, datasetName, tableName):
        parser = reqparse.RequestParser()
        parser.add_argument("token", type=str, help="variable 1", location="args")
        args = parser.parse_args()
        token = args["token"]
        #stringId = args["stringId"] == "true"
       
        if not validate_token(token):
            return {"Error": "Invalid Token"}, 403

        query = {"datasetName": datasetName, "tableName": tableName}
        try:
            result = row_c.delete_many(query)
            table_c.delete_one(query)
            cea_c.delete_many(query)
            cta_c.delete_many(query)
            cpa_c.delete_many(query)
            candidate_scored_c.delete_many(query)
            return {"datasetName": datasetName, "tableName": tableName, "deleted": True}, 200
        except Exception as e:
            print({"traceback": traceback.format_exc()}, flush=True)
            return {"status": "Error", "message": str(e)}, 400


if __name__ == "__main__":
    app.run(debug=True)
