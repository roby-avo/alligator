import sys
import os

# Add the parent directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from fastapi import FastAPI
from app.api import api_endpoints

app = FastAPI()

# Include the API router
app.include_router(api_endpoints.router)