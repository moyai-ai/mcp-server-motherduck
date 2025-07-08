#!/usr/bin/env python3
"""
Lightweight MotherDuck MCP Server with optimized startup and memory usage.
This version uses lazy loading and minimal imports for faster startup.
"""

import sys
import os
import logging
from typing import Optional

# Set up minimal logging first
logging.basicConfig(
    level=logging.INFO, 
    format="[motherduck] %(levelname)s - %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("mcp_server_motherduck")

def lazy_import_fastmcp():
    """Lazy import FastMCP components to reduce startup time"""
    try:
        from .fastmcp_server import create_fastmcp_server, DatabaseConfig
        return create_fastmcp_server, DatabaseConfig
    except ImportError as e:
        logger.error(f"Failed to import FastMCP: {e}")
        logger.error("Please install fastmcp: pip install fastmcp")
        sys.exit(1)

def create_optimized_server(
    db_path: str = "md:",
    motherduck_token: Optional[str] = None,
    home_dir: Optional[str] = None,
    saas_mode: bool = False,
    read_only: bool = False,
    transport: str = "stdio",
    port: int = 8000,
    host: str = "0.0.0.0"
):
    """Create an optimized MCP server with minimal memory footprint"""
    
    # Get version without importing entire config module
    version = "0.6.2"
    
    logger.info(f"🦆 MotherDuck MCP Server v{version} (Optimized)")
    logger.info("Ready to execute SQL queries via DuckDB/MotherDuck")
    
    # Lazy import FastMCP components
    create_fastmcp_server, DatabaseConfig = lazy_import_fastmcp()
    
    # Create database configuration
    config = DatabaseConfig(
        db_path=db_path,
        motherduck_token=motherduck_token,
        home_dir=home_dir,
        saas_mode=saas_mode,
        read_only=read_only,
    )
    
    # Create FastMCP server with optimized settings
    app = create_fastmcp_server(config)
    
    # Run server based on transport
    if transport == "sse":
        logger.info("MCP server initialized with FastMCP in \\033[32msse\\033[0m mode")
        logger.info(f"🦆 Connect to MotherDuck MCP Server at \\033[1m\\033[36mhttp://{host}:{port}/sse\\033[0m")
        app.run(transport="sse", host=host, port=port)
    elif transport == "stream":
        logger.info("MCP server initialized with FastMCP in \\033[32mhttp\\033[0m mode")
        logger.info(f"🦆 Connect to MotherDuck MCP Server at \\033[1m\\033[36mhttp://{host}:{port}/mcp\\033[0m")
        app.run(transport="http", host=host, port=port)
    else:
        logger.info("MCP server initialized with FastMCP in \\033[32mstdio\\033[0m mode")
        logger.info("Waiting for client connection")
        app.run(transport="stdio")

def main():
    """Lightweight main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="MotherDuck MCP Server (Optimized)")
    parser.add_argument("--port", type=int, default=8000, help="Port to listen on")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--transport", choices=["stdio", "sse", "stream"], default="stdio", help="Transport type")
    parser.add_argument("--db-path", default="md:", help="Database path")
    parser.add_argument("--motherduck-token", help="MotherDuck token")
    parser.add_argument("--home-dir", help="Home directory for DuckDB")
    parser.add_argument("--saas-mode", action="store_true", help="MotherDuck SaaS mode")
    parser.add_argument("--read-only", action="store_true", help="Read-only mode")
    
    args = parser.parse_args()
    
    # Get token from environment if not provided
    motherduck_token = args.motherduck_token or os.getenv("motherduck_token")
    
    create_optimized_server(
        db_path=args.db_path,
        motherduck_token=motherduck_token,
        home_dir=args.home_dir,
        saas_mode=args.saas_mode,
        read_only=args.read_only,
        transport=args.transport,
        port=args.port,
        host=args.host
    )

if __name__ == "__main__":
    main()