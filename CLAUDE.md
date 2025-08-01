# Airtable Gateway Service - Claude Context

## 🎯 Service Purpose
This is the **foundational data layer** of the PyAirtable ecosystem. It provides a clean, secure, and performant REST API wrapper around the Airtable API, handling all direct Airtable interactions for the entire microservices architecture.

## 🏗️ Current State
- **Core Functionality**: ✅ Complete CRUD operations for Airtable records
- **Authentication**: ⚠️ Simple API key (needs JWT upgrade)
- **Caching**: ✅ Redis caching with smart invalidation implemented
- **Rate Limiting**: ✅ Redis-based distributed rate limiting implemented
- **Testing**: ❌ No tests yet
- **Monitoring**: ⚠️ Basic logging + cache health monitoring

## 🔧 Technical Details
- **Framework**: FastAPI 0.115.5
- **Python**: 3.12 with full async/await
- **HTTP Client**: pyAirtable SDK (official)
- **Database**: PostgreSQL (for future caching)
- **Cache**: Redis (configured but unused)

## 📋 API Endpoints
```
GET    /health                                     # Health check
GET    /bases                                      # List all accessible bases
GET    /bases/{base_id}/schema                     # Get base schema with tables
GET    /bases/{base_id}/tables/{table_id}/records  # List records
POST   /bases/{base_id}/tables/{table_id}/records  # Create record
PATCH  /bases/{base_id}/tables/{table_id}/records/{record_id}  # Update record
DELETE /bases/{base_id}/tables/{table_id}/records/{record_id}  # Delete record
POST   /bases/{base_id}/tables/{table_id}/records/batch        # Batch create
```

## 🚀 Immediate Priorities
1. **Add Retry Logic** (MEDIUM)
   - Exponential backoff for 429 errors
   - Circuit breaker for repeated failures

2. **Rate Limiting** ✅ (COMPLETED)
   - Respect Airtable's 5 QPS limit ✅
   - Add per-base rate limiting ✅  
   - Global rate limiting (100 QPM) ✅
   - Proper HTTP headers and 429 responses ✅

3. **Redis Caching** ✅ (COMPLETED)
   - Cache base schemas (1 hour TTL) ✅
   - Cache frequently accessed records (5 min TTL) ✅
   - Implement cache invalidation on writes ✅

## 🔮 Future Enhancements
### Phase 1 (Next Sprint)
- [ ] Comprehensive error handling with custom exceptions
- [ ] Request/response validation with Pydantic models
- [ ] Correlation ID tracking across requests
- [ ] Structured logging with context

### Phase 2 (Next Month)
- [ ] Connection pooling optimization
- [ ] Batch operations for better performance
- [ ] Webhook support for real-time updates
- [ ] Advanced filtering with query builder

### Phase 3 (Future)
- [ ] Multi-tenant support with base isolation
- [ ] Advanced caching strategies (write-through, write-behind)
- [ ] GraphQL endpoint option
- [ ] Airtable formula validation

## ⚠️ Known Issues
1. **No input validation** - Direct JSON processing
2. **Formula injection risk** - User input not sanitized
3. **No connection pooling** - Using default httpx client
4. **Missing health checks** - Only basic endpoint exists

## 🧪 Testing Strategy
```python
# Priority test coverage needed:
- Unit tests for each endpoint
- Integration tests with mock Airtable
- Load tests for rate limiting
- Security tests for injection vulnerabilities
```

## 🔒 Security TODOs
- [ ] Replace simple API key with JWT tokens
- [ ] Add input sanitization for formulas
- [ ] Implement request signing
- [ ] Add audit logging for all operations

## 📊 Performance Targets
- **Response Time**: < 200ms (excluding Airtable API)
- **Cache Hit Rate**: > 80% for schema queries
- **Throughput**: 50 requests/second
- **Availability**: 99.9% uptime

## 🤝 Service Dependencies
- **Upstream**: MCP Server (primary consumer)
- **Downstream**: Airtable API
- **Infrastructure**: Redis (caching), PostgreSQL (future)

## 💡 Development Tips
1. Always check Airtable rate limits before making requests
2. Use batch operations when possible
3. Cache aggressively but invalidate smartly
4. Log all Airtable API errors for debugging

## 🚨 Critical Configuration
```python
# Required environment variables:
AIRTABLE_TOKEN=your_personal_access_token  # NEVER commit this!
API_KEY=service_api_key                    # Internal service auth
REDIS_URL=redis://redis:6379              # Cache connection
```

Remember: This service is the **single source of truth** for all Airtable data access. Every optimization here benefits the entire system!