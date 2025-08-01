"""
Airtable Gateway Service
Simple REST API wrapper for Airtable operations
"""

import os
import sys
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from pyairtable import Api
from pyairtable.exceptions import HttpError
from dotenv import load_dotenv
import logging

from cache import cache_manager, create_query_hash
from rate_limiter import check_rate_limits

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))
logger = logging.getLogger(__name__)

# Secure configuration imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../pyairtable-common'))

try:
    from pyairtable_common.config import initialize_secrets, get_secret, close_secrets, ConfigurationError
    from pyairtable_common.middleware import setup_security_middleware, verify_api_key_secure
    SECURE_CONFIG_AVAILABLE = True
except ImportError as e:
    logger.warning(f"⚠️ Secure configuration not available: {e}")
    SECURE_CONFIG_AVAILABLE = False
    # Fallback security functions
    def verify_api_key_secure(provided, expected):
        return provided == expected

# Initialize secure configuration manager
config_manager = None
if SECURE_CONFIG_AVAILABLE:
    try:
        config_manager = initialize_secrets()
        logger.info("✅ Secure configuration manager initialized")
    except Exception as e:
        logger.error(f"💥 Failed to initialize secure configuration: {e}")
        raise

# Configuration with secure secret retrieval
AIRTABLE_TOKEN = None
API_KEY = None

if config_manager:
    try:
        AIRTABLE_TOKEN = get_secret("AIRTABLE_TOKEN")
        API_KEY = get_secret("API_KEY")
    except Exception as e:
        logger.error(f"💥 Failed to get secrets from secure config: {e}")
        raise ValueError("Required secrets could not be retrieved from secure configuration")
else:
    # Fallback to environment variables
    AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
    API_KEY = os.getenv("API_KEY")
    
    if not API_KEY:
        logger.error("💥 CRITICAL: API_KEY environment variable is required")
        raise ValueError("API_KEY environment variable is required")
    
    if not AIRTABLE_TOKEN:
        raise ValueError("AIRTABLE_TOKEN environment variable is required")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting Airtable Gateway Service...")
    await cache_manager.connect()
    yield
    # Shutdown
    logger.info("Shutting down Airtable Gateway Service...")
    await cache_manager.disconnect()
    if config_manager:
        await close_secrets()
        logger.info("Closed secure configuration manager")


# Initialize FastAPI app
app = FastAPI(
    title="Airtable Gateway",
    description="Pure Python Airtable API wrapper service",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware with security hardening
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://localhost:8000").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-API-Key"],
)

# Add security middleware
if SECURE_CONFIG_AVAILABLE:
    setup_security_middleware(app, rate_limit_calls=300, rate_limit_period=60)

# Initialize Airtable client
airtable = Api(AIRTABLE_TOKEN)


def verify_api_key(x_api_key: Optional[str] = Header(None)) -> bool:
    """Secure API key verification with constant-time comparison"""
    if not verify_api_key_secure(x_api_key or "", API_KEY or ""):
        raise HTTPException(status_code=401, detail="Invalid API key")
    return True


async def check_airtable_limits(base_id: str, api_key: str) -> None:
    """Check Airtable API rate limits and raise HTTPException if exceeded."""
    rate_check = await check_rate_limits(base_id, api_key)
    
    if not rate_check["allowed"]:
        result = rate_check["result"]
        limit_type = rate_check["limit_type"]
        
        logger.warning(
            f"Airtable {limit_type} rate limit exceeded",
            base_id=base_id,
            limit=result["limit"],
            retry_after=result["retry_after"]
        )
        
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit exceeded for {limit_type} ({result['limit']} requests per {result['window_seconds']}s)",
            headers={
                "X-RateLimit-Limit": str(result["limit"]),
                "X-RateLimit-Remaining": str(result["remaining"]),
                "X-RateLimit-Reset": str(int(result["reset_time"])),
                "Retry-After": str(result["retry_after"]),
                "X-RateLimit-Type": limit_type
            }
        )


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    cache_health = await cache_manager.health_check()
    return {
        "status": "healthy", 
        "service": "airtable-gateway",
        "cache": cache_health
    }


@app.get("/bases")
async def list_bases(x_api_key: Optional[str] = Header(None)):
    """List all accessible Airtable bases"""
    verify_api_key(x_api_key)
    
    # Check global rate limit (using a dummy base_id for global limit)
    await check_airtable_limits("global", AIRTABLE_TOKEN)
    
    # Try cache first
    cached_bases = await cache_manager.get_bases()
    if cached_bases:
        logger.info(f"Retrieved {len(cached_bases)} bases from cache")
        return {"bases": cached_bases}
    
    try:
        bases = []
        for base in airtable.bases:
            bases.append({
                "id": base.id,
                "name": base.name,
                "permission_level": base.permission_level
            })
        
        # Cache the result
        await cache_manager.set_bases(bases)
        
        logger.info(f"Retrieved {len(bases)} bases from Airtable API")
        return {"bases": bases}
    
    except Exception as e:
        logger.error(f"Error listing bases: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/bases/{base_id}/schema")
async def get_base_schema(base_id: str, x_api_key: Optional[str] = Header(None)):
    """Get schema for a specific base including all tables"""
    verify_api_key(x_api_key)
    
    # Check rate limits
    await check_airtable_limits(base_id, AIRTABLE_TOKEN)
    
    # Try cache first
    cached_schema = await cache_manager.get_schema(base_id)
    if cached_schema:
        logger.info(f"Retrieved schema for base {base_id} from cache")
        return cached_schema
    
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
        
        result = {"base_id": base_id, "tables": tables}
        
        # Cache the result
        await cache_manager.set_schema(base_id, result)
        
        logger.info(f"Retrieved schema for base {base_id} with {len(tables)} tables from Airtable API")
        return result
    
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
    
    # Check rate limits
    await check_airtable_limits(base_id, AIRTABLE_TOKEN)
    
    # Create query hash for caching
    query_hash = create_query_hash(max_records, view, filter_by_formula, sort)
    
    # Try cache first
    cached_records = await cache_manager.get_records(base_id, table_id, query_hash)
    if cached_records:
        logger.info(f"Retrieved {len(cached_records)} records from cache for {base_id}/{table_id}")
        return {"records": cached_records}
    
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
        
        # Cache the result
        await cache_manager.set_records(base_id, table_id, query_hash, records)
        
        logger.info(f"Retrieved {len(records)} records from Airtable API for {base_id}/{table_id}")
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
    
    # Check rate limits
    await check_airtable_limits(base_id, AIRTABLE_TOKEN)
    
    try:
        table = airtable.table(base_id, table_id)
        record = table.create(fields)
        
        # Invalidate cache for this table
        await cache_manager.invalidate_table(base_id, table_id)
        
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
    
    # Check rate limits
    await check_airtable_limits(base_id, AIRTABLE_TOKEN)
    
    try:
        table = airtable.table(base_id, table_id)
        record = table.update(record_id, fields)
        
        # Invalidate cache for this table
        await cache_manager.invalidate_table(base_id, table_id)
        
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
    
    # Check rate limits
    await check_airtable_limits(base_id, AIRTABLE_TOKEN)
    
    try:
        table = airtable.table(base_id, table_id)
        deleted = table.delete(record_id)
        
        # Invalidate cache for this table
        await cache_manager.invalidate_table(base_id, table_id)
        
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
    
    # Check rate limits
    await check_airtable_limits(base_id, AIRTABLE_TOKEN)
    
    try:
        table = airtable.table(base_id, table_id)
        created_records = table.batch_create(records)
        
        # Invalidate cache for this table
        await cache_manager.invalidate_table(base_id, table_id)
        
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