from pydantic import BaseModel, Field
from typing import List, Dict

class Table(BaseModel):
    name: str
    schema_: Dict = Field(..., alias='schema')
    rows: List[Dict] = []

class Dataset(BaseModel):
    name: str
    tables: List[Table] = []