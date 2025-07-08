import os
import duckdb
from typing import Literal, Optional
import io
from contextlib import redirect_stdout
from tabulate import tabulate
import logging
import asyncio
import time
from .configs import SERVER_VERSION

logger = logging.getLogger("mcp_server_motherduck")


class DatabaseClient:
    def __init__(
        self,
        db_path: str | None = None,
        motherduck_token: str | None = None,
        home_dir: str | None = None,
        saas_mode: bool = False,
        read_only: bool = False,
    ):
        self._read_only = read_only
        self.db_path, self.db_type = self._resolve_db_path_type(
            db_path, motherduck_token, saas_mode
        )
        logger.info(f"Database client initialized in `{self.db_type}` mode")

        # Set the home directory for DuckDB
        if home_dir:
            os.environ["HOME"] = home_dir

        self.conn = None
        self._connection_lock = asyncio.Lock()
        self._last_health_check = 0
        self._health_check_interval = 30  # seconds
        self._max_retries = 3
        self._retry_delay = 1  # seconds
        
        # Use single attach mode for MotherDuck to prevent connection issues
        self._connection_config = {
            "custom_user_agent": f"mcp-server-motherduck/{SERVER_VERSION}",
        }
        if self.db_type == "motherduck":
            self._connection_config["enable_external_access"] = True
            self._connection_config["enable_fsst_vectors"] = False
            # Add single attach mode to connection string
            if "?" in self.db_path:
                self.db_path += "&single_attach=true"
            else:
                self.db_path += "?single_attach=true"
            logger.info("Enabled single attach mode for MotherDuck connection")
            
        self.conn = self._initialize_connection()

    def _initialize_connection(self) -> Optional[duckdb.DuckDBPyConnection]:
        """Initialize connection to the MotherDuck or DuckDB database"""

        logger.info(f"🔌 Connecting to {self.db_type} database")

        if self.db_type == "duckdb" and self._read_only:
            # For read-only local DuckDB, we'll use short-lived connections
            try:
                conn = duckdb.connect(
                    self.db_path,
                    config=self._connection_config,
                    read_only=self._read_only,
                )
                conn.execute("SELECT 1")
                conn.close()
                return None
            except Exception as e:
                logger.error(f"❌ Read-only check failed: {e}")
                raise

        # For MotherDuck, maintain persistent connection with health checks
        conn = duckdb.connect(
            self.db_path,
            config=self._connection_config,
            read_only=self._read_only,
        )

        logger.info(f"✅ Successfully connected to {self.db_type} database")
        return conn

    async def _health_check(self) -> bool:
        """Check if the database connection is healthy"""
        if self.conn is None:
            return True  # Short-lived connections are always "healthy"
            
        current_time = time.time()
        if current_time - self._last_health_check < self._health_check_interval:
            return True
            
        try:
            self.conn.execute("SELECT 1")
            self._last_health_check = current_time
            return True
        except Exception as e:
            logger.warning(f"Health check failed: {e}")
            return False

    async def _ensure_connection(self) -> duckdb.DuckDBPyConnection:
        """Ensure we have a healthy connection, reconnect if necessary"""
        async with self._connection_lock:
            if self.conn is None:
                # Short-lived connection for read-only local DuckDB
                return duckdb.connect(
                    self.db_path,
                    config=self._connection_config,
                    read_only=self._read_only,
                )
                
            # Check connection health
            if not await self._health_check():
                logger.info("🔄 Reconnecting to database due to failed health check")
                try:
                    self.conn.close()
                except:
                    pass
                
                for attempt in range(self._max_retries):
                    try:
                        self.conn = self._initialize_connection()
                        if self.conn:
                            logger.info(f"✅ Reconnected successfully (attempt {attempt + 1})")
                            break
                    except Exception as e:
                        logger.warning(f"Reconnection attempt {attempt + 1} failed: {e}")
                        if attempt < self._max_retries - 1:
                            await asyncio.sleep(self._retry_delay * (2 ** attempt))
                        else:
                            raise
                            
            return self.conn

    def _resolve_db_path_type(
        self, db_path: str, motherduck_token: str | None = None, saas_mode: bool = False
    ) -> tuple[str, Literal["duckdb", "motherduck"]]:
        """Resolve and validate the database path"""
        # Handle MotherDuck paths
        if db_path.startswith("md:"):
            if motherduck_token:
                logger.info("Using MotherDuck token to connect to database `md:`")
                if saas_mode:
                    logger.info("Connecting to MotherDuck in SaaS mode")
                    return (
                        f"{db_path}?motherduck_token={motherduck_token}&saas_mode=true",
                        "motherduck",
                    )
                else:
                    return (
                        f"{db_path}?motherduck_token={motherduck_token}",
                        "motherduck",
                    )
            elif os.getenv("motherduck_token"):
                logger.info(
                    "Using MotherDuck token from env to connect to database `md:`"
                )
                return (
                    f"{db_path}?motherduck_token={os.getenv('motherduck_token')}",
                    "motherduck",
                )
            else:
                raise ValueError(
                    "Please set the `motherduck_token` as an environment variable or pass it as an argument with `--motherduck-token` when using `md:` as db_path."
                )

        if db_path == ":memory:":
            return db_path, "duckdb"

        return db_path, "duckdb"

    async def _execute(self, query: str) -> str:
        conn = await self._ensure_connection()
        is_temp_conn = self.conn is None  # Check if we're using temporary connection
        
        try:
            q = conn.execute(query)
            out = tabulate(
                q.fetchall(),
                headers=[d[0] + "\n" + d[1] for d in q.description],
                tablefmt="pretty",
            )
            return out
        finally:
            if is_temp_conn:
                conn.close()

    async def query(self, query: str) -> str:
        try:
            return await self._execute(query)
        except Exception as e:
            raise ValueError(f"❌ Error executing query: {e}")
    
    def query_sync(self, query: str) -> str:
        """Synchronous wrapper for backward compatibility"""
        if self.conn is None:
            # For read-only mode, use short-lived connection
            conn = duckdb.connect(
                self.db_path,
                config=self._connection_config,
                read_only=self._read_only,
            )
            try:
                q = conn.execute(query)
                out = tabulate(
                    q.fetchall(),
                    headers=[d[0] + "\n" + d[1] for d in q.description],
                    tablefmt="pretty",
                )
                return out
            finally:
                conn.close()
        else:
            # For persistent connections, use existing connection
            try:
                q = self.conn.execute(query)
                out = tabulate(
                    q.fetchall(),
                    headers=[d[0] + "\n" + d[1] for d in q.description],
                    tablefmt="pretty",
                )
                return out
            except Exception as e:
                raise ValueError(f"❌ Error executing query: {e}")
    
    def close(self):
        """Close the database connection"""
        if self.conn:
            try:
                self.conn.close()
            except:
                pass
            self.conn = None
