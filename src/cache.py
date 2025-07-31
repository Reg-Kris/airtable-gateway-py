"""
Redis cache manager for Airtable Gateway Service
"""
import json
import os
import time
from typing import Any, Optional, Dict, List
from datetime import timedelta
import logging

import redis.asyncio as redis
from redis.asyncio import Redis

logger = logging.getLogger(__name__)


class CacheManager:
    """Redis cache manager with type-specific TTL and invalidation."""
    
    def __init__(self, redis_url: str = None):
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379")
        self.client: Optional[Redis] = None
        
        # Cache TTL configurations
        self.ttl_config = {
            "bases": timedelta(hours=4),           # Base list changes infrequently
            "schema": timedelta(hours=1),          # Schema changes rarely
            "records": timedelta(minutes=5),       # Records change frequently
            "record": timedelta(minutes=2),        # Single record
        }
    
    async def connect(self):
        """Initialize Redis connection."""
        try:
            self.client = redis.from_url(self.redis_url, decode_responses=True)
            await self.client.ping()
            logger.info(f"Connected to Redis at {self.redis_url}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.client = None
    
    async def disconnect(self):
        """Close Redis connection."""
        if self.client:
            await self.client.close()
            logger.info("Disconnected from Redis")
    
    def _make_key(self, key_type: str, *args) -> str:
        """Generate consistent cache keys."""
        return f"airtable:{key_type}:{':'.join(str(arg) for arg in args)}"
    
    async def get(self, key_type: str, *args) -> Optional[Any]:
        """Get cached value."""
        if not self.client:
            return None
        
        key = self._make_key(key_type, *args)
        try:
            cached = await self.client.get(key)
            if cached:
                logger.debug(f"Cache HIT: {key}")
                return json.loads(cached)
            else:
                logger.debug(f"Cache MISS: {key}")
                return None
        except Exception as e:
            logger.error(f"Cache read error for key {key}: {e}")
            return None
    
    async def set(self, key_type: str, value: Any, *args, ttl: Optional[timedelta] = None) -> bool:
        """Set cached value with TTL."""
        if not self.client:
            return False
        
        key = self._make_key(key_type, *args)
        ttl = ttl or self.ttl_config.get(key_type, timedelta(minutes=5))
        
        try:
            await self.client.setex(
                key,
                int(ttl.total_seconds()),
                json.dumps(value, default=str)
            )
            logger.debug(f"Cache SET: {key} (TTL: {ttl})")
            return True
        except Exception as e:
            logger.error(f"Cache write error for key {key}: {e}")
            return False
    
    async def delete(self, key_type: str, *args) -> bool:
        """Delete cached value."""
        if not self.client:
            return False
        
        key = self._make_key(key_type, *args)
        try:
            result = await self.client.delete(key)
            logger.debug(f"Cache DELETE: {key}")
            return result > 0
        except Exception as e:
            logger.error(f"Cache delete error for key {key}: {e}")
            return False
    
    async def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate all keys matching pattern."""
        if not self.client:
            return 0
        
        try:
            keys = await self.client.keys(f"airtable:{pattern}")
            if keys:
                result = await self.client.delete(*keys)
                logger.info(f"Invalidated {result} keys matching pattern: {pattern}")
                return result
            return 0
        except Exception as e:
            logger.error(f"Cache pattern invalidation error for {pattern}: {e}")
            return 0
    
    async def get_bases(self) -> Optional[List[Dict[str, Any]]]:
        """Get cached bases."""
        return await self.get("bases")
    
    async def set_bases(self, bases: List[Dict[str, Any]]) -> bool:
        """Cache bases list."""
        return await self.set("bases", bases)
    
    async def get_schema(self, base_id: str) -> Optional[Dict[str, Any]]:
        """Get cached base schema."""
        return await self.get("schema", base_id)
    
    async def set_schema(self, base_id: str, schema: Dict[str, Any]) -> bool:
        """Cache base schema."""
        return await self.set("schema", schema, base_id)
    
    async def get_records(self, base_id: str, table_id: str, query_hash: str) -> Optional[List[Dict[str, Any]]]:
        """Get cached records for specific query."""
        return await self.get("records", base_id, table_id, query_hash)
    
    async def set_records(self, base_id: str, table_id: str, query_hash: str, records: List[Dict[str, Any]]) -> bool:
        """Cache records for specific query."""
        return await self.set("records", records, base_id, table_id, query_hash)
    
    async def get_record(self, base_id: str, table_id: str, record_id: str) -> Optional[Dict[str, Any]]:
        """Get cached single record."""
        return await self.get("record", base_id, table_id, record_id)
    
    async def set_record(self, base_id: str, table_id: str, record_id: str, record: Dict[str, Any]) -> bool:
        """Cache single record."""
        return await self.set("record", record, base_id, table_id, record_id)
    
    async def invalidate_table(self, base_id: str, table_id: str):
        """Invalidate all cached data for a table."""
        await self.invalidate_pattern(f"records:{base_id}:{table_id}:*")
        await self.invalidate_pattern(f"record:{base_id}:{table_id}:*")
    
    async def invalidate_base(self, base_id: str):
        """Invalidate all cached data for a base."""
        await self.invalidate_pattern(f"*:{base_id}:*")
    
    async def health_check(self) -> Dict[str, Any]:
        """Check cache health."""
        if not self.client:
            return {"status": "disconnected", "error": "No Redis connection"}
        
        try:
            start_time = time.time()
            await self.client.ping()
            latency = (time.time() - start_time) * 1000
            
            info = await self.client.info("memory")
            return {
                "status": "healthy",
                "latency_ms": round(latency, 2),
                "memory_used": info.get("used_memory_human", "unknown"),
                "connected_clients": info.get("connected_clients", 0),
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}


def create_query_hash(max_records: int, view: Optional[str], formula: Optional[str], sort: Optional[List[str]]) -> str:
    """Create a hash for query parameters to use as cache key."""
    import hashlib
    
    query_params = {
        "max_records": max_records,
        "view": view,
        "formula": formula,
        "sort": sort
    }
    
    query_str = json.dumps(query_params, sort_keys=True, default=str)
    return hashlib.md5(query_str.encode()).hexdigest()[:12]


# Global cache instance
cache_manager = CacheManager()