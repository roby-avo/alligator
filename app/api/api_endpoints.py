import sys
import os

# Add the parent directory to the system path
#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from fastapi import APIRouter, HTTPException, UploadFile, File, Depends, Request
from typing import List, Dict, Any
import pandas as pd
from models.models import Dataset, Table
from utils.mongodb_utils import MongoDBHandler
from utils.auth import authenticate_token
from utils.ip_tracing import trace_ip

router = APIRouter()

# Initialize MongoDB handler
mongodb_handler = MongoDBHandler(uri="mongodb://alligator_mongo:27017", db_name="alligator")

# Authentication dependency
def get_current_user(token: str = Depends(authenticate_token)):
    return token

@router.post("/datasets/", response_model=Dict)
def create_dataset(dataset: Dataset, token: str = Depends(get_current_user), request: Request = Depends(trace_ip)):
    try:
        return mongodb_handler.create_dataset(dataset.name)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/datasets/", response_model=List[str])
def get_datasets(token: str = Depends(get_current_user), request: Request = Depends(trace_ip)):
    return mongodb_handler.get_datasets()

@router.get("/datasets/{dataset_name}", response_model=Dict)
def get_dataset(dataset_name: str, token: str = Depends(get_current_user), request: Request = Depends(trace_ip)):
    try:
        return mongodb_handler.get_dataset(dataset_name)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.delete("/datasets/{dataset_name}", response_model=Dict)
def delete_dataset(dataset_name: str, token: str = Depends(get_current_user), request: Request = Depends(trace_ip)):
    try:
        return mongodb_handler.delete_dataset(dataset_name)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.post("/datasets/{dataset_name}/tables/", response_model=Dict)
def create_table(dataset_name: str, table: Table, token: str = Depends(get_current_user), request: Request = Depends(trace_ip)):
    try:
        return mongodb_handler.create_table(dataset_name, table.name, table.schema_)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/datasets/{dataset_name}/tables/", response_model=List[Dict])
def get_tables(dataset_name: str, token: str = Depends(get_current_user), request: Request = Depends(trace_ip)):
    try:
        return mongodb_handler.get_tables(dataset_name)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.get("/datasets/{dataset_name}/tables/{table_name}", response_model=Dict)
def get_table(dataset_name: str, table_name: str, token: str = Depends(get_current_user), request: Request = Depends(trace_ip)):
    try:
        return mongodb_handler.get_table(dataset_name, table_name)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.delete("/datasets/{dataset_name}/tables/{table_name}", response_model=Dict)
def delete_table(dataset_name: str, table_name: str, token: str = Depends(get_current_user), request: Request = Depends(trace_ip)):
    try:
        return mongodb_handler.delete_table(dataset_name, table_name)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.post("/datasets/{dataset_name}/tables/{table_name}/upload_json", response_model=Dict)
def upload_table_json(dataset_name: str, table_name: str, table: Table, token: str = Depends(get_current_user), request: Request = Depends(trace_ip)):
    try:
        mongodb_handler.create_table(dataset_name, table_name, table.schema_)
        for row in table.rows:
            mongodb_handler.add_row_to_table(dataset_name, table_name, row)
        return {"message": "Table uploaded successfully in JSON format"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/datasets/{dataset_name}/tables/{table_name}/upload_csv", response_model=Dict)
def upload_table_csv(dataset_name: str, table_name: str, file: UploadFile = File(...), columns: str = "", token: str = Depends(get_current_user), request: Request = Depends(trace_ip)):
    try:
        df = pd.read_csv(file.file)
        schema = {col: columns.split(',')[i] for i, col in enumerate(df.columns)}
        mongodb_handler.create_table(dataset_name, table_name, schema)
        for _, row in df.iterrows():
            mongodb_handler.add_row_to_table(dataset_name, table_name, row.to_dict())
        return {"message": "Table uploaded successfully in CSV format"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))