#!/usr/bin/env python3
"""
Airtable Gateway Web API Usage Examples
This file demonstrates how to use the new Web API endpoints for dynamic base and table creation.
"""

import httpx
import asyncio
import json

# Configuration
GATEWAY_URL = "http://localhost:8002"
API_KEY = "your-api-key-here"  # Replace with your actual API key
HEADERS = {
    "X-API-Key": API_KEY,
    "Content-Type": "application/json"
}


async def example_list_bases():
    """Example: List all accessible bases using Web API"""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{GATEWAY_URL}/api/web/bases", headers=HEADERS)
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        return response.json()


async def example_create_base():
    """Example: Create a new base with initial tables"""
    base_data = {
        "name": "My New Project Base",
        "workspace_id": "wsp1234567890abcdef",  # Replace with actual workspace ID
        "tables": [
            {
                "name": "Tasks",
                "fields": [
                    {
                        "name": "Task Name",
                        "type": "singleLineText"
                    },
                    {
                        "name": "Status",
                        "type": "singleSelect",
                        "options": {
                            "choices": [
                                {"name": "Todo", "color": "redBright"},
                                {"name": "In Progress", "color": "yellowBright"},
                                {"name": "Done", "color": "greenBright"}
                            ]
                        }
                    },
                    {
                        "name": "Due Date",
                        "type": "date",
                        "options": {
                            "dateFormat": {"name": "iso"},
                            "timeFormat": {"name": "24hour"}
                        }
                    },
                    {
                        "name": "Priority",
                        "type": "number",
                        "options": {
                            "precision": 0
                        }
                    }
                ]
            }
        ]
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{GATEWAY_URL}/api/web/bases",
            headers=HEADERS,
            json=base_data
        )
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        return response.json()


async def example_get_base_schema(base_id: str):
    """Example: Get detailed base schema with all tables and fields"""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{GATEWAY_URL}/api/web/bases/{base_id}/tables",
            headers=HEADERS
        )
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        return response.json()


async def example_create_table(base_id: str):
    """Example: Create a new table in an existing base"""
    table_data = {
        "name": "Team Members",
        "description": "Track team member information and roles",
        "fields": [
            {
                "name": "Name",
                "type": "singleLineText",
                "description": "Full name of the team member"
            },
            {
                "name": "Email",
                "type": "email",
                "description": "Contact email address"
            },
            {
                "name": "Role",
                "type": "singleSelect",
                "options": {
                    "choices": [
                        {"name": "Developer", "color": "blueBright"},
                        {"name": "Designer", "color": "purpleBright"},
                        {"name": "Manager", "color": "greenBright"},
                        {"name": "QA", "color": "orangeBright"}
                    ]
                }
            },
            {
                "name": "Start Date",
                "type": "date",
                "options": {
                    "dateFormat": {"name": "iso"}
                }
            },
            {
                "name": "Active",
                "type": "checkbox",
                "description": "Is this team member currently active?"
            }
        ]
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{GATEWAY_URL}/api/web/bases/{base_id}/tables",
            headers=HEADERS,
            json=table_data
        )
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        return response.json()


async def example_create_field(base_id: str, table_id: str):
    """Example: Add a new field to an existing table"""
    field_data = {
        "name": "Years of Experience",
        "type": "number",
        "description": "Number of years of professional experience",
        "options": {
            "precision": 1
        }
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{GATEWAY_URL}/api/web/bases/{base_id}/tables/{table_id}/fields",
            headers=HEADERS,
            json=field_data
        )
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        return response.json()


async def example_update_table(base_id: str, table_id: str):
    """Example: Update table metadata"""
    update_data = {
        "name": "Updated Team Members",
        "description": "Updated description for team member tracking"
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.patch(
            f"{GATEWAY_URL}/api/web/bases/{base_id}/tables/{table_id}",
            headers=HEADERS,
            json=update_data
        )
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        return response.json()


async def example_get_field_templates():
    """Example: Get field creation templates"""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{GATEWAY_URL}/api/web/field-templates",
            headers=HEADERS
        )
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
        return response.json()


async def complete_workflow_example():
    """Example: Complete workflow - create base, add table, add field"""
    print("🚀 Starting complete Web API workflow example...")
    
    try:
        # Step 1: List existing bases
        print("\n1. Listing existing bases...")
        bases = await example_list_bases()
        
        # Step 2: Get field templates for reference
        print("\n2. Getting field templates...")
        await example_get_field_templates()
        
        # Step 3: Create a new base (commented out to avoid creating test bases)
        # print("\n3. Creating new base...")
        # new_base = await example_create_base()
        # base_id = new_base.get('id')
        
        # For demo purposes, use an existing base ID
        if bases.get('bases'):
            base_id = bases['bases'][0]['id']
            print(f"\n3. Using existing base: {base_id}")
            
            # Step 4: Get base schema
            print("\n4. Getting base schema...")
            schema = await example_get_base_schema(base_id)
            
            # Step 5: Create a new table (commented out to avoid creating test tables)
            # print("\n5. Creating new table...")
            # new_table = await example_create_table(base_id)
            # table_id = new_table.get('id')
            
            # For demo purposes, use an existing table ID if available
            tables = schema.get('tables', [])
            if tables:
                table_id = tables[0]['id']
                print(f"\n5. Using existing table: {table_id}")
                
                # Step 6: Add a field to the table (commented out to avoid modifying tables)
                # print("\n6. Adding new field to table...")
                # await example_create_field(base_id, table_id)
                
                # Step 7: Update table metadata (commented out to avoid modifying tables)
                # print("\n7. Updating table metadata...")
                # await example_update_table(base_id, table_id)
                
                print("\n✅ Workflow example completed successfully!")
            else:
                print("\n⚠️ No tables found in base")
        else:
            print("\n⚠️ No bases found")
            
    except Exception as e:
        print(f"\n❌ Error in workflow: {e}")


if __name__ == "__main__":
    print("Airtable Gateway Web API Examples")
    print("=" * 50)
    print("Make sure the gateway is running on http://localhost:8002")
    print("Update API_KEY variable with your actual API key")
    print("Update workspace_id in create_base example with your actual workspace ID")
    print()
    
    # Run the complete workflow example
    asyncio.run(complete_workflow_example())