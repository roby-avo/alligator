from fastapi import HTTPException, Security
from fastapi.security.api_key import APIKeyHeader

API_KEY = "secret-api-key"  # Replace with your actual API key
API_KEY_NAME = "access_token"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

async def authenticate_token(api_key: str = Security(api_key_header)):
    if api_key == API_KEY:
        return api_key
    else:
        raise HTTPException(status_code=403, detail="Could not validate credentials")
