"""
Airtable Gateway Service - Refactored with PyAirtableService Base Class
Simple REST API wrapper for Airtable operations
"""

import os
import sys
from typing import Optional, Dict, Any, List

from fastapi import HTTPException, Header, Query, Depends
from pyairtable import Api
from pyairtable.exceptions import HttpError

# Add pyairtable-common to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../pyairtable-common'))

from pyairtable_common.service import PyAirtableService, ServiceConfig

from cache import cache_manager, create_query_hash
from rate_limiter import check_rate_limits

# Secure configuration imports
try:
    from pyairtable_common.config import initialize_secrets, get_secret, close_secrets, ConfigurationError
    SECURE_CONFIG_AVAILABLE = True
except ImportError:
    SECURE_CONFIG_AVAILABLE = False


class AirtableGatewayService(PyAirtableService):
    """
    Airtable Gateway service extending PyAirtableService base class.
    """
    
    def __init__(self):
        # Initialize secure configuration manager
        self.config_manager = None
        self.airtable_token = None
        
        if SECURE_CONFIG_AVAILABLE:
            try:
                self.config_manager = initialize_secrets()
                self.airtable_token = get_secret("AIRTABLE_TOKEN")
                api_key = get_secret("API_KEY")
                self.logger.info("✅ Secure configuration manager initialized")
            except Exception as e:
                self.logger.error(f"💥 Failed to initialize secure configuration: {e}")
                raise
        else:
            # Fallback to environment variables
            self.airtable_token = os.getenv("AIRTABLE_TOKEN")
            api_key = os.getenv("API_KEY")
            
            if not api_key:
                self.logger.error("💥 CRITICAL: API_KEY environment variable is required")
                raise ValueError("API_KEY environment variable is required")
            
            if not self.airtable_token:
                raise ValueError("AIRTABLE_TOKEN environment variable is required")
        
        # Initialize service configuration
        config = ServiceConfig(
            title="Airtable Gateway",
            description="Pure Python Airtable API wrapper service",
            version="1.0.0",
            service_name="airtable-gateway",
            port=int(os.getenv("PORT", 8002)),
            api_key=api_key,
            rate_limit_calls=300,  # Moderate rate limit for Airtable operations
            rate_limit_period=60,
            startup_tasks=[self._connect_cache],
            shutdown_tasks=[self._disconnect_cache, self._close_secrets]
        )
        
        super().__init__(config)
        
        # Initialize Airtable client
        self.airtable = Api(self.airtable_token)
        
        # Setup routes
        self._setup_airtable_routes()
    
    async def _connect_cache(self) -> None:
        """Connect to cache manager."""
        await cache_manager.connect()
        self.logger.info("✅ Cache manager connected")
    
    async def _disconnect_cache(self) -> None:
        """Disconnect from cache manager."""
        await cache_manager.disconnect()
        self.logger.info("✅ Cache manager disconnected")
    
    async def _close_secrets(self) -> None:
        """Close secure configuration manager."""
        if self.config_manager:
            await close_secrets()
            self.logger.info("✅ Closed secure configuration manager")
    
    async def _check_airtable_limits(self, base_id: str, api_key: str) -> None:
        """Check Airtable API rate limits and raise HTTPException if exceeded."""
        rate_check = await check_rate_limits(base_id, api_key)
        
        if not rate_check["allowed"]:
            result = rate_check["result"]
            limit_type = rate_check["limit_type"]
            
            self.logger.warning(
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
    
    def _setup_airtable_routes(self) -> None:
        """Setup Airtable Gateway specific routes."""
        
        @self.app.get("/bases")
        async def list_bases(authenticated: bool = Depends(self.verify_api_key)):
            """List all accessible Airtable bases"""
            
            # Check global rate limit (using a dummy base_id for global limit)
            await self._check_airtable_limits("global", self.airtable_token)
            
            # Try cache first
            cached_bases = await cache_manager.get_bases()
            if cached_bases:
                self.logger.info(f"Retrieved {len(cached_bases)} bases from cache")
                return {"bases": cached_bases}
            
            try:
                bases = []
                for base in self.airtable.bases:
                    bases.append({
                        "id": base.id,
                        "name": base.name,
                        "permission_level": base.permission_level
                    })
                
                # Cache the result
                await cache_manager.set_bases(bases)
                
                self.logger.info(f"Retrieved {len(bases)} bases from Airtable API")
                return {"bases": bases}
            
            except Exception as e:
                self.logger.error(f"Error listing bases: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/bases/{base_id}/schema")
        async def get_base_schema(base_id: str, authenticated: bool = Depends(self.verify_api_key)):
            """Get schema for a specific base including all tables"""
            
            # Check rate limits
            await self._check_airtable_limits(base_id, self.airtable_token)
            
            # Try cache first
            cached_schema = await cache_manager.get_schema(base_id)
            if cached_schema:
                self.logger.info(f"Retrieved schema for base {base_id} from cache")
                return cached_schema
            
            try:
                base = self.airtable.base(base_id)
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
                
                self.logger.info(f"Retrieved schema for base {base_id} with {len(tables)} tables from Airtable API")
                return result
            
            except Exception as e:
                self.logger.error(f"Error getting base schema: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.get("/bases/{base_id}/tables/{table_id}/records")
        async def list_records(
            base_id: str,
            table_id: str,
            authenticated: bool = Depends(self.verify_api_key),
            max_records: int = Query(100, ge=1, le=1000),
            view: Optional[str] = Query(None),
            filter_by_formula: Optional[str] = Query(None),
            sort: Optional[List[str]] = Query(None)
        ):
            """List records from a table with optional filtering"""
            
            # Check rate limits
            await self._check_airtable_limits(base_id, self.airtable_token)
            
            # Create query hash for caching
            query_hash = create_query_hash(max_records, view, filter_by_formula, sort)
            
            # Try cache first
            cached_records = await cache_manager.get_records(base_id, table_id, query_hash)
            if cached_records:
                self.logger.info(f"Retrieved {len(cached_records)} records from cache for {base_id}/{table_id}")
                return {"records": cached_records}
            
            try:
                table = self.airtable.table(base_id, table_id)
                
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
                
                self.logger.info(f"Retrieved {len(records)} records from Airtable API for {base_id}/{table_id}")
                return {"records": records}
            
            except HttpError as e:
                self.logger.error(f"Airtable API error: {e}")
                raise HTTPException(status_code=e.status_code, detail=e.message)
            except Exception as e:
                self.logger.error(f"Error listing records: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/bases/{base_id}/tables/{table_id}/records")
        async def create_record(
            base_id: str,
            table_id: str,
            fields: Dict[str, Any],
            authenticated: bool = Depends(self.verify_api_key)
        ):
            """Create a new record in a table"""
            
            # Check rate limits
            await self._check_airtable_limits(base_id, self.airtable_token)
            
            try:
                table = self.airtable.table(base_id, table_id)
                record = table.create(fields)
                
                # Invalidate cache for this table
                await cache_manager.invalidate_table(base_id, table_id)
                
                self.logger.info(f"Created record {record['id']} in {base_id}/{table_id}")
                return {
                    "id": record["id"],
                    "fields": record["fields"],
                    "createdTime": record["createdTime"]
                }
            
            except HttpError as e:
                self.logger.error(f"Airtable API error: {e}")
                raise HTTPException(status_code=e.status_code, detail=e.message)
            except Exception as e:
                self.logger.error(f"Error creating record: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.patch("/bases/{base_id}/tables/{table_id}/records/{record_id}")
        async def update_record(
            base_id: str,
            table_id: str,
            record_id: str,
            fields: Dict[str, Any],
            authenticated: bool = Depends(self.verify_api_key)
        ):
            """Update an existing record"""
            
            # Check rate limits
            await self._check_airtable_limits(base_id, self.airtable_token)
            
            try:
                table = self.airtable.table(base_id, table_id)
                record = table.update(record_id, fields)
                
                # Invalidate cache for this table
                await cache_manager.invalidate_table(base_id, table_id)
                
                self.logger.info(f"Updated record {record_id} in {base_id}/{table_id}")
                return {
                    "id": record["id"],
                    "fields": record["fields"],
                    "createdTime": record["createdTime"]
                }
            
            except HttpError as e:
                self.logger.error(f"Airtable API error: {e}")
                raise HTTPException(status_code=e.status_code, detail=e.message)
            except Exception as e:
                self.logger.error(f"Error updating record: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.delete("/bases/{base_id}/tables/{table_id}/records/{record_id}")
        async def delete_record(
            base_id: str,
            table_id: str,
            record_id: str,
            authenticated: bool = Depends(self.verify_api_key)
        ):
            """Delete a record"""
            
            # Check rate limits
            await self._check_airtable_limits(base_id, self.airtable_token)
            
            try:
                table = self.airtable.table(base_id, table_id)
                deleted = table.delete(record_id)
                
                # Invalidate cache for this table
                await cache_manager.invalidate_table(base_id, table_id)
                
                self.logger.info(f"Deleted record {record_id} from {base_id}/{table_id}")
                return {"deleted": True, "id": record_id}
            
            except HttpError as e:
                self.logger.error(f"Airtable API error: {e}")
                raise HTTPException(status_code=e.status_code, detail=e.message)
            except Exception as e:
                self.logger.error(f"Error deleting record: {e}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.app.post("/bases/{base_id}/tables/{table_id}/records/batch")
        async def create_records_batch(
            base_id: str,
            table_id: str,
            records: List[Dict[str, Any]],
            authenticated: bool = Depends(self.verify_api_key)
        ):
            """Create multiple records in a single request"""
            
            # Check rate limits
            await self._check_airtable_limits(base_id, self.airtable_token)
            
            try:
                table = self.airtable.table(base_id, table_id)
                created_records = table.batch_create(records)
                
                # Invalidate cache for this table
                await cache_manager.invalidate_table(base_id, table_id)
                
                self.logger.info(f"Created {len(created_records)} records in {base_id}/{table_id}")
                return {"records": created_records}
            
            except HttpError as e:
                self.logger.error(f"Airtable API error: {e}")
                raise HTTPException(status_code=e.status_code, detail=e.message)
            except Exception as e:
                self.logger.error(f"Error batch creating records: {e}")
                raise HTTPException(status_code=500, detail=str(e))
    
    async def health_check(self) -> Dict[str, Any]:
        """Custom health check for Airtable Gateway."""
        cache_health = await cache_manager.health_check()
        return {
            "cache": cache_health,
            "airtable_connected": bool(self.airtable)
        }


def create_airtable_gateway_service() -> AirtableGatewayService:
    """Factory function to create Airtable Gateway service."""
    return AirtableGatewayService()


if __name__ == "__main__":
    service = create_airtable_gateway_service()
    service.run()