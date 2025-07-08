import logging
import asyncio
from typing import Optional
from fastmcp import FastMCP
from pydantic import BaseModel, Field
from .database import DatabaseClient
from .configs import SERVER_VERSION
from .prompt import PROMPT_TEMPLATE

logger = logging.getLogger("mcp_server_motherduck")

class DatabaseConfig(BaseModel):
    db_path: str = Field(default="md:", description="Path to local DuckDB database file or MotherDuck database")
    motherduck_token: Optional[str] = Field(default=None, description="Access token for MotherDuck database connections")
    home_dir: Optional[str] = Field(default=None, description="Home directory for DuckDB")
    saas_mode: bool = Field(default=False, description="Flag for connecting to MotherDuck in SaaS mode")
    read_only: bool = Field(default=False, description="Flag for connecting to DuckDB in read-only mode")

def create_fastmcp_server(config: DatabaseConfig) -> FastMCP:
    """Create a FastMCP server"""
    
    # Create FastMCP instance
    mcp = FastMCP(
        name="mcp-server-motherduck",
        version=SERVER_VERSION,
    )
    
    # Global database client
    db_client = DatabaseClient(
        db_path=config.db_path,
        motherduck_token=config.motherduck_token,
        home_dir=config.home_dir,
        saas_mode=config.saas_mode,
        read_only=config.read_only,
    )
    
    @mcp.prompt("duckdb-motherduck-initial-prompt")
    async def get_prompt(arguments: Optional[dict] = None):
        """Get a specific prompt"""
        logger.info(f"Getting prompt: duckdb-motherduck-initial-prompt with arguments: {arguments}")
        
        return {
            "description": "Initial prompt for interacting with DuckDB/MotherDuck",
            "messages": [
                {
                    "role": "user",
                    "content": {
                        "type": "text",
                        "text": PROMPT_TEMPLATE
                    }
                }
            ]
        }
    
    @mcp.tool("query")
    async def query(query: str):
        """Execute a SQL query on the database"""
        logger.info(f"Executing query: {query}")
        
        try:
            # Use async query method if available, otherwise fall back to sync
            if hasattr(db_client, 'query') and asyncio.iscoroutinefunction(db_client.query):
                result = await db_client.query(query)
            else:
                result = db_client.query_sync(query)
            
            return str(result)
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise ValueError(f"Error executing query: {str(e)}")
    
    return mcp