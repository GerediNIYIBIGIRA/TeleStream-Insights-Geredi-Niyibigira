"""
A minimal API key authentication dependency for FastAPI. Replace with your chosen
authentication mechanism in production.
"""

from fastapi import HTTPException, Security
from fastapi.security.api_key import APIKeyHeader
from starlette.status import HTTP_403_FORBIDDEN

API_KEY_HEADER = "X-API-KEY"
api_key_header = APIKeyHeader(name=API_KEY_HEADER, auto_error=False)


def get_api_key(api_key_header: str = Security(api_key_header)):
    if api_key_header == "secret-token":
        return api_key_header
    raise HTTPException(
        status_code=HTTP_403_FORBIDDEN, detail="Could not validate credentials"
    )
