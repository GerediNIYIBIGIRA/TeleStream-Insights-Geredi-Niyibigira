"""
Pydantic models for the FastAPI service.
"""

from pydantic import BaseModel
from typing import Optional


class Record(BaseModel):
    event_id: int
    value: str
    ts: int


class IngestResponse(BaseModel):
    accepted: int
    failed: int
