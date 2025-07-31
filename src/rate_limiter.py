"""
Rate limiting for Airtable Gateway Service
"""
import time
import hashlib
from typing import Dict, Any
import logging

import redis.asyncio as redis
from redis.asyncio import Redis

from cache import cache_manager

logger = logging.getLogger(__name__)


class AirtableRateLimiter:
    """Airtable-specific rate limiter respecting API limits."""
    
    def __init__(self, redis_client: Redis = None):
        self.redis = redis_client or cache_manager.client
        self.prefix = "airtable_rate_limit"
    
    def _make_key(self, identifier: str) -> str:
        """Generate rate limit key."""
        return f"{self.prefix}:{identifier}"
    
    async def check_base_limit(self, base_id: str) -> Dict[str, Any]:
        """Check rate limit for specific Airtable base (5 QPS)."""
        return await self._sliding_window_check(
            identifier=f"base:{base_id}",
            limit=5,
            window_seconds=1
        )
    
    async def check_global_limit(self, api_key: str) -> Dict[str, Any]:
        """Check global Airtable API limit per API key (100 requests per minute)."""
        api_key_hash = hashlib.sha256(api_key.encode()).hexdigest()[:12]
        return await self._sliding_window_check(
            identifier=f"global:{api_key_hash}",
            limit=100,
            window_seconds=60
        )
    
    async def _sliding_window_check(self, identifier: str, limit: int, window_seconds: int) -> Dict[str, Any]:
        """Sliding window rate limiter using Redis sorted sets."""
        if not self.redis:
            # If Redis is not available, allow all requests
            return {
                "allowed": True,
                "remaining": limit - 1,
                "reset_time": time.time() + window_seconds,
                "retry_after": 0,
                "limit": limit,
                "window_seconds": window_seconds
            }
        
        key = self._make_key(identifier)
        now = time.time()
        window_start = now - window_seconds
        
        try:
            pipe = self.redis.pipeline()
            
            # Remove expired entries
            pipe.zremrangebyscore(key, 0, window_start)
            
            # Count current requests
            pipe.zcard(key)
            
            # Add current request
            pipe.zadd(key, {str(now): now})
            
            # Set expiration
            pipe.expire(key, window_seconds)
            
            results = await pipe.execute()
            current_requests = results[1]
            
            if current_requests >= limit:
                # Remove the request we just added since it's not allowed
                await self.redis.zrem(key, str(now))
                
                # Get the oldest request to calculate reset time
                oldest = await self.redis.zrange(key, 0, 0, withscores=True)
                reset_time = oldest[0][1] + window_seconds if oldest else now + window_seconds
                
                logger.warning(
                    f"Rate limit exceeded for {identifier}",
                    current_requests=current_requests,
                    limit=limit,
                    window_seconds=window_seconds
                )
                
                return {
                    "allowed": False,
                    "remaining": 0,
                    "reset_time": reset_time,
                    "retry_after": int(reset_time - now),
                    "limit": limit,
                    "window_seconds": window_seconds
                }
            
            remaining = limit - current_requests - 1
            reset_time = now + window_seconds
            
            return {
                "allowed": True,
                "remaining": remaining,
                "reset_time": reset_time,
                "retry_after": 0,
                "limit": limit,
                "window_seconds": window_seconds
            }
            
        except Exception as e:
            logger.error(f"Rate limiting error: {e}")
            # Allow request if Redis fails
            return {
                "allowed": True,
                "remaining": limit - 1,
                "reset_time": time.time() + window_seconds,
                "retry_after": 0,
                "limit": limit,
                "window_seconds": window_seconds
            }
    
    async def reset_limits(self, identifier: str):
        """Reset rate limits for identifier."""
        if not self.redis:
            return 0
        
        try:
            keys = await self.redis.keys(f"{self.prefix}:*{identifier}*")
            if keys:
                deleted = await self.redis.delete(*keys)
                logger.info(f"Reset rate limits for {identifier}, deleted {deleted} keys")
                return deleted
            return 0
        except Exception as e:
            logger.error(f"Error resetting rate limits: {e}")
            return 0


# Global rate limiter instance
rate_limiter = AirtableRateLimiter()


async def check_rate_limits(base_id: str, api_key: str) -> Dict[str, Any]:
    """
    Check both global and base-specific rate limits.
    
    Returns:
        Dict with rate limit status and which limit was hit if any
    """
    
    # Check global limit first (100 requests per minute)
    global_result = await rate_limiter.check_global_limit(api_key)
    if not global_result["allowed"]:
        return {
            "allowed": False,
            "limit_type": "global",
            "result": global_result
        }
    
    # Check base-specific limit (5 requests per second)
    base_result = await rate_limiter.check_base_limit(base_id)
    if not base_result["allowed"]:
        return {
            "allowed": False,
            "limit_type": "base",
            "result": base_result
        }
    
    return {
        "allowed": True,
        "global_result": global_result,
        "base_result": base_result
    }