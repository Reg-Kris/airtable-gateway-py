"""
Airtable Gateway Service
Simple REST API wrapper for Airtable operations
"""

import os
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from pyairtable import Api
from pyairtable.exceptions import HttpError
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))
logger = logging.getLogger(__name__)

# Configuration
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
API_KEY = os.getenv("API_KEY", "simple-api-key")

if not AIRTABLE_TOKEN:
    raise ValueError("AIRTABLE_TOKEN environment variable is required")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting Airtable Gateway Service...")
    yield
    # Shutdown
    logger.info("Shutting down Airtable Gateway Service...")


# Initialize FastAPI app
app = FastAPI(
    title="Airtable Gateway",
    description="Pure Python Airtable API wrapper service",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Airtable client
airtable = Api(AIRTABLE_TOKEN)


def verify_api_key(x_api_key: Optional[str] = Header(None)) -> bool:
    """Simple API key verification"""
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "airtable-gateway"}


@app.get("/bases")
async def list_bases(x_api_key: Optional[str] = Header(None)):
    """List all accessible Airtable bases"""
    verify_api_key(x_api_key)
    
    try:
        bases = []
        for base in airtable.bases:
            bases.append({
                "id": base.id,
                "name": base.name,
                "permission_level": base.permission_level
            })
        
        logger.info(f"Retrieved {len(bases)} bases")
        return {"bases": bases}
    
    except Exception as e:
        logger.error(f"Error listing bases: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/bases/{base_id}/schema")
async def get_base_schema(base_id: str, x_api_key: Optional[str] = Header(None)):
    """Get schema for a specific base including all tables"""
    verify_api_key(x_api_key)
    
    try:
        base = airtable.base(base_id)
        schema = base.schema()
        
        tables = []
        for table in schema.tables:
            fields = []
            for field in table.fields:
                fields.append({
                    "id": field.id,
                    "name": field.name,
                    "type": field.type,
                    "description": getattr(field, 'description', None)
                })
            
            tables.append({
                "id": table.id,
                "name": table.name,
                "description": getattr(table, 'description', None),
                "fields": fields,
                "views": [{"id": view.id, "name": view.name} for view in table.views]
            })
        
        logger.info(f"Retrieved schema for base {base_id} with {len(tables)} tables")
        return {"base_id": base_id, "tables": tables}
    
    except Exception as e:
        logger.error(f"Error getting base schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/bases/{base_id}/tables/{table_id}/records")
async def list_records(
    base_id: str,
    table_id: str,
    x_api_key: Optional[str] = Header(None),
    max_records: int = Query(100, ge=1, le=1000),
    view: Optional[str] = Query(None),
    filter_by_formula: Optional[str] = Query(None),
    sort: Optional[List[str]] = Query(None)
):
    """List records from a table with optional filtering"""
    verify_api_key(x_api_key)
    
    try:
        table = airtable.table(base_id, table_id)
        
        # Build query parameters
        kwargs = {"max_records": max_records}
        if view:
            kwargs["view"] = view
        if filter_by_formula:
            kwargs["formula"] = filter_by_formula
        if sort:
            kwargs["sort"] = sort
        
        records = []
        for record in table.all(**kwargs):
            records.append({
                "id": record["id"],
                "fields": record["fields"],
                "createdTime": record["createdTime"]
            })
        
        logger.info(f"Retrieved {len(records)} records from {base_id}/{table_id}")
        return {"records": records}
    
    except HttpError as e:
        logger.error(f"Airtable API error: {e}")
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.error(f"Error listing records: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/bases/{base_id}/tables/{table_id}/records")
async def create_record(
    base_id: str,
    table_id: str,
    fields: Dict[str, Any],
    x_api_key: Optional[str] = Header(None)
):
    """Create a new record in a table"""
    verify_api_key(x_api_key)
    
    try:
        table = airtable.table(base_id, table_id)
        record = table.create(fields)
        
        logger.info(f"Created record {record['id']} in {base_id}/{table_id}")
        return {
            "id": record["id"],
            "fields": record["fields"],
            "createdTime": record["createdTime"]
        }
    
    except HttpError as e:
        logger.error(f"Airtable API error: {e}")
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.error(f"Error creating record: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/bases/{base_id}/tables/{table_id}/records/{record_id}")
async def update_record(
    base_id: str,
    table_id: str,
    record_id: str,
    fields: Dict[str, Any],
    x_api_key: Optional[str] = Header(None)
):
    """Update an existing record"""
    verify_api_key(x_api_key)
    
    try:
        table = airtable.table(base_id, table_id)
        record = table.update(record_id, fields)
        
        logger.info(f"Updated record {record_id} in {base_id}/{table_id}")
        return {
            "id": record["id"],
            "fields": record["fields"],
            "createdTime": record["createdTime"]
        }
    
    except HttpError as e:
        logger.error(f"Airtable API error: {e}")
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.error(f"Error updating record: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/bases/{base_id}/tables/{table_id}/records/{record_id}")
async def delete_record(
    base_id: str,
    table_id: str,
    record_id: str,
    x_api_key: Optional[str] = Header(None)
):
    """Delete a record"""
    verify_api_key(x_api_key)
    
    try:
        table = airtable.table(base_id, table_id)
        deleted = table.delete(record_id)
        
        logger.info(f"Deleted record {record_id} from {base_id}/{table_id}")
        return {"deleted": True, "id": record_id}
    
    except HttpError as e:
        logger.error(f"Airtable API error: {e}")
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.error(f"Error deleting record: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/bases/{base_id}/tables/{table_id}/records/batch")
async def create_records_batch(
    base_id: str,
    table_id: str,
    records: List[Dict[str, Any]],
    x_api_key: Optional[str] = Header(None)
):
    """Create multiple records in a single request"""
    verify_api_key(x_api_key)
    
    try:
        table = airtable.table(base_id, table_id)
        created_records = table.batch_create(records)
        
        logger.info(f"Created {len(created_records)} records in {base_id}/{table_id}")
        return {"records": created_records}
    
    except HttpError as e:
        logger.error(f"Airtable API error: {e}")
        raise HTTPException(status_code=e.status_code, detail=e.message)
    except Exception as e:
        logger.error(f"Error batch creating records: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8002)))