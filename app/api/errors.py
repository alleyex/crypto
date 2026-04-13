"""Shared HTTP error helpers for route handlers."""
from typing import NoReturn

from fastapi import HTTPException

def http_not_found(detail: str) -> NoReturn:
    raise HTTPException(status_code=404, detail=detail)

def http_unprocessable(detail: str) -> NoReturn:
    raise HTTPException(status_code=422, detail=detail)

def http_bad_request(detail: str) -> NoReturn:
    raise HTTPException(status_code=400, detail=detail)

def http_bad_gateway(detail: str) -> NoReturn:
    raise HTTPException(status_code=502, detail=detail)
