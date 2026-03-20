from pydantic import BaseModel, HttpUrl
from typing import Optional, List
from datetime import datetime


class SearchRequest(BaseModel):
    query: str                        # company name or URL
    force_refresh: bool = False       # skip cache


class OwnerResultOut(BaseModel):
    owner_name: Optional[str] = None
    role: Optional[str] = None
    phone_numbers: List[str] = []
    email: Optional[str] = None
    linkedin_url: Optional[str] = None
    source_name: str
    source_url: Optional[str] = None
    confidence_score: float = 0.5
    raw_snippet: Optional[str] = None

    class Config:
        from_attributes = True


class SearchResponse(BaseModel):
    query: str
    company_name: Optional[str] = None
    company_url: Optional[str] = None
    found: bool
    results: List[OwnerResultOut]
    from_cache: bool = False
    message: Optional[str] = None


class StreamEvent(BaseModel):
    event: str   # "status" | "result" | "done" | "error"
    data: dict
