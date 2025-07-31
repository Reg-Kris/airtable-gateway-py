# airtable-gateway-py

Pure Python Airtable API wrapper service - no MCP, no LLM, just clean Airtable operations

## Overview

This microservice provides a clean REST API interface to Airtable operations using the pyAirtable SDK. It handles:
- Authentication with Airtable
- Rate limiting (respecting Airtable's 5 QPS limit)
- Basic CRUD operations on records
- Schema discovery and base exploration

## Quick Start

```bash
# Clone the repository
git clone https://github.com/Reg-Kris/airtable-gateway-py.git
cd airtable-gateway-py

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set environment variables
cp .env.example .env
# Edit .env with your Airtable credentials

# Run the service
uvicorn src.main:app --reload --port 8002
```

## API Endpoints

- `GET /health` - Health check
- `GET /bases` - List all accessible bases
- `GET /bases/{base_id}/schema` - Get base schema with tables
- `GET /bases/{base_id}/tables/{table_id}/records` - List records
- `POST /bases/{base_id}/tables/{table_id}/records` - Create record
- `PATCH /bases/{base_id}/tables/{table_id}/records/{record_id}` - Update record
- `DELETE /bases/{base_id}/tables/{table_id}/records/{record_id}` - Delete record

## Environment Variables

```
AIRTABLE_TOKEN=your_personal_access_token
AIRTABLE_BASE=your_default_base_id (optional)
```

## Testing

```bash
# Run tests
pytest

# Test with curl
curl http://localhost:8002/health
curl http://localhost:8002/bases -H "X-API-Key: your-api-key"
```

## Docker

```bash
# Build image
docker build -t airtable-gateway-py .

# Run container
docker run -p 8002:8002 --env-file .env airtable-gateway-py
```