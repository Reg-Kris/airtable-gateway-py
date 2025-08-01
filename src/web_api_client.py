"""
Airtable Web API Client
Handles Web API operations for dynamic table and base creation
"""
import httpx
import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class WebAPIError(Exception):
    """Custom exception for Web API errors"""
    status_code: int
    message: str
    error_type: str = "UNKNOWN_ERROR"


class AirtableWebAPIClient:
    """Client for Airtable Web API operations"""
    
    def __init__(self, personal_access_token: str):
        self.pat = personal_access_token
        self.base_url = "https://api.airtable.com/v0/meta"
        self.headers = {
            "Authorization": f"Bearer {self.pat}",
            "Content-Type": "application/json"
        }
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers=self.headers
        )
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()
    
    def _handle_response(self, response: httpx.Response) -> Dict[str, Any]:
        """Handle Web API response and raise appropriate errors"""
        if response.status_code >= 400:
            try:
                error_data = response.json()
                error_message = error_data.get("error", {}).get("message", "Unknown error")
                error_type = error_data.get("error", {}).get("type", "UNKNOWN_ERROR")
            except Exception:
                error_message = f"HTTP {response.status_code}: {response.text}"
                error_type = "HTTP_ERROR"
            
            logger.error(f"Web API error: {response.status_code} - {error_message}")
            raise WebAPIError(
                status_code=response.status_code,
                message=error_message,
                error_type=error_type
            )
        
        return response.json()
    
    async def list_bases(self) -> Dict[str, Any]:
        """List all accessible bases"""
        try:
            response = await self.client.get(f"{self.base_url}/bases")
            return self._handle_response(response)
        except Exception as e:
            logger.error(f"Error listing bases: {e}")
            raise
    
    async def create_base(self, name: str, workspace_id: str, tables: List[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create a new base"""
        payload = {
            "name": name,
            "workspaceId": workspace_id
        }
        
        if tables:
            payload["tables"] = tables
        
        try:
            response = await self.client.post(f"{self.base_url}/bases", json=payload)
            return self._handle_response(response)
        except Exception as e:
            logger.error(f"Error creating base: {e}")
            raise
    
    async def get_base_schema(self, base_id: str) -> Dict[str, Any]:
        """Get base schema including all tables and fields"""
        try:
            response = await self.client.get(f"{self.base_url}/bases/{base_id}/tables")
            return self._handle_response(response)
        except Exception as e:
            logger.error(f"Error getting base schema: {e}")
            raise
    
    async def create_table(self, base_id: str, name: str, fields: List[Dict[str, Any]], description: str = None) -> Dict[str, Any]:
        """Create a new table in a base"""
        payload = {
            "name": name,
            "fields": fields
        }
        
        if description:
            payload["description"] = description
        
        try:
            response = await self.client.post(f"{self.base_url}/bases/{base_id}/tables", json=payload)
            return self._handle_response(response)
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            raise
    
    async def update_table(self, base_id: str, table_id: str, name: str = None, description: str = None) -> Dict[str, Any]:
        """Update table metadata"""
        payload = {}
        
        if name:
            payload["name"] = name
        if description:
            payload["description"] = description
        
        if not payload:
            raise ValueError("At least one field (name or description) must be provided")
        
        try:
            response = await self.client.patch(f"{self.base_url}/bases/{base_id}/tables/{table_id}", json=payload)
            return self._handle_response(response)
        except Exception as e:
            logger.error(f"Error updating table: {e}")
            raise
    
    async def create_field(self, base_id: str, table_id: str, field_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new field in a table"""
        try:
            response = await self.client.post(
                f"{self.base_url}/bases/{base_id}/tables/{table_id}/fields", 
                json=field_data
            )
            return self._handle_response(response)
        except Exception as e:
            logger.error(f"Error creating field: {e}")
            raise
    
    async def update_field(self, base_id: str, table_id: str, field_id: str, field_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing field"""
        try:
            response = await self.client.patch(
                f"{self.base_url}/bases/{base_id}/tables/{table_id}/fields/{field_id}",
                json=field_data
            )
            return self._handle_response(response)
        except Exception as e:
            logger.error(f"Error updating field: {e}")
            raise
    
    async def delete_field(self, base_id: str, table_id: str, field_id: str) -> Dict[str, Any]:
        """Delete a field from a table"""
        try:
            response = await self.client.delete(
                f"{self.base_url}/bases/{base_id}/tables/{table_id}/fields/{field_id}"
            )
            return self._handle_response(response)
        except Exception as e:
            logger.error(f"Error deleting field: {e}")
            raise


# Helper functions for field creation
def create_text_field(name: str, description: str = None) -> Dict[str, Any]:
    """Create a single line text field"""
    field = {
        "name": name,
        "type": "singleLineText"
    }
    if description:
        field["description"] = description
    return field


def create_multiline_text_field(name: str, description: str = None) -> Dict[str, Any]:
    """Create a long text field"""
    field = {
        "name": name,
        "type": "multilineText"
    }
    if description:
        field["description"] = description
    return field


def create_number_field(name: str, precision: int = 0, description: str = None) -> Dict[str, Any]:
    """Create a number field"""
    field = {
        "name": name,
        "type": "number",
        "options": {
            "precision": precision
        }
    }
    if description:
        field["description"] = description
    return field


def create_select_field(name: str, choices: List[Dict[str, str]], description: str = None) -> Dict[str, Any]:
    """Create a single select field"""
    field = {
        "name": name,
        "type": "singleSelect",
        "options": {
            "choices": choices
        }
    }
    if description:
        field["description"] = description
    return field


def create_multiselect_field(name: str, choices: List[Dict[str, str]], description: str = None) -> Dict[str, Any]:
    """Create a multiple select field"""
    field = {
        "name": name,
        "type": "multipleSelects",
        "options": {
            "choices": choices
        }
    }
    if description:
        field["description"] = description
    return field


def create_date_field(name: str, include_time: bool = False, description: str = None) -> Dict[str, Any]:
    """Create a date field"""
    field = {
        "name": name,
        "type": "date",
        "options": {
            "dateFormat": {
                "name": "iso"
            }
        }
    }
    
    if include_time:
        field["options"]["timeFormat"] = {
            "name": "24hour"
        }
    
    if description:
        field["description"] = description
    return field


def create_checkbox_field(name: str, description: str = None) -> Dict[str, Any]:
    """Create a checkbox field"""
    field = {
        "name": name,
        "type": "checkbox"
    }
    if description:
        field["description"] = description
    return field


def create_url_field(name: str, description: str = None) -> Dict[str, Any]:
    """Create a URL field"""
    field = {
        "name": name,
        "type": "url"
    }
    if description:
        field["description"] = description
    return field


def create_email_field(name: str, description: str = None) -> Dict[str, Any]:
    """Create an email field"""
    field = {
        "name": name,
        "type": "email"
    }
    if description:
        field["description"] = description
    return field