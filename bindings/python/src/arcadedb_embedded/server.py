"""
ArcadeDB Python Bindings - Server Management

ArcadeDB server for HTTP API and Studio web interface.
"""

import os
from typing import Any, Dict, Optional

from .core import Database
from .exceptions import ArcadeDBError
from .jvm import start_jvm


class ArcadeDBServer:
    """ArcadeDB Server wrapper for enabling HTTP API and Studio access."""

    def __init__(
        self,
        root_path: str = "./databases",
        root_password: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize ArcadeDB server.

        Args:
            root_path: Root directory for databases (default: ./databases)
            root_password: Root user password (optional, recommended for
                production)
            config: Optional configuration dictionary with keys like:
                - http_port: HTTP API port (default: 2480)
                - binary_port: Binary protocol port (default: 2424)
                - host: Host to bind to (default: 0.0.0.0)
                - mode: Server mode (default: development)
        """
        start_jvm()
        from com.arcadedb import ContextConfiguration
        from com.arcadedb.server import ArcadeDBServer as JavaArcadeDBServer

        self._config = config or {}
        self._root_path = root_path
        self._root_password = root_password
        self._java_server = None
        self._started = False

        # Create configuration
        context_config = ContextConfiguration()

        # Set root path
        context_config.setValue("arcadedb.server.rootPath", root_path)

        # Set root password if provided
        if root_password:
            context_config.setValue("arcadedb.server.rootPassword", root_password)

        # Set common defaults
        mode = self._config.get("mode", "development")
        context_config.setValue("arcadedb.server.mode", mode)

        host = self._config.get("host", "0.0.0.0")
        context_config.setValue("arcadedb.server.httpIncoming.host", host)

        http_port = self._config.get("http_port", 2480)
        context_config.setValue("arcadedb.server.httpIncoming.port", http_port)

        # Apply any additional configuration
        for key, value in self._config.items():
            if key not in ["mode", "host", "http_port", "binary_port"]:
                # Convert Python config keys to ArcadeDB config format
                config_key = f"arcadedb.{key.replace('_', '.')}"
                context_config.setValue(config_key, value)

        # Create server instance
        self._java_server = JavaArcadeDBServer(context_config)

    def start(self):
        """Start the ArcadeDB server."""
        if self._started:
            raise ArcadeDBError("Server is already started")

        try:
            self._java_server.start()
            self._started = True
        except Exception as e:
            raise ArcadeDBError(f"Failed to start server: {e}") from e

    def stop(self):
        """Stop the ArcadeDB server."""
        if not self._started:
            return

        try:
            self._java_server.stop()
            self._started = False
        except Exception as e:
            raise ArcadeDBError(f"Failed to stop server: {e}") from e

    def __del__(self):
        """Finalizer - ensure server is stopped."""
        try:
            if self._started and self._java_server is not None:
                self._java_server.stop()
                self._started = False
        except Exception:
            pass  # Ignore errors during garbage collection

    def get_database(self, name: str) -> Database:
        """
        Get a database instance from the server.

        Args:
            name: Database name

        Returns:
            Database instance
        """
        if not self._started:
            raise ArcadeDBError("Server is not started")

        try:
            java_db = self._java_server.getDatabase(name)
            return Database(java_db)
        except Exception as e:
            raise ArcadeDBError(f"Failed to get database '{name}': {e}") from e

    def create_database(self, name: str) -> Database:
        """
        Create a new database on the server.

        This uses the server's built-in database management, which properly
        registers the database and makes it immediately visible in Studio.

        Args:
            name: Database name

        Returns:
            Database instance
        """
        if not self._started:
            raise ArcadeDBError("Server is not started")

        try:
            # Use server's getDatabase with createIfNotExists=True
            # This properly registers the database with the server
            java_db = self._java_server.getDatabase(name, True, True)
            return Database(java_db)
        except Exception as e:
            raise ArcadeDBError(f"Failed to create database '{name}': {e}") from e

    def is_started(self) -> bool:
        """Check if server is running."""
        return self._started

    def get_http_port(self) -> int:
        """Get the HTTP port the server is listening on."""
        return self._config.get("http_port", 2480)

    def get_studio_url(self) -> str:
        """Get the URL for the Studio web interface."""
        host = self._config.get("host", "localhost")
        if host == "0.0.0.0":
            host = "localhost"
        port = self.get_http_port()
        return f"http://{host}:{port}/"

    def __enter__(self):
        """Context manager entry - starts the server."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - stops the server."""
        self.stop()


def create_server(
    root_path: str = "./databases",
    root_password: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
) -> ArcadeDBServer:
    """
    Create an ArcadeDB server instance.

    Args:
        root_path: Root directory for databases (default: ./databases)
        root_password: Root user password (optional, recommended for
            production)
        config: Optional configuration dictionary

    Returns:
        ArcadeDBServer instance

    Note - Log File Locations:
        ArcadeDB writes logs to multiple locations:

        1. Application logs (arcadedb.log.*):
           - Location: ./log/ (relative to working directory)
           - Cannot be changed without modifying Java code

        2. Server event logs (server-event-log-*.jsonl):
           - Location: {root_path}/log/ (e.g., ./databases/log/)
           - Keeps event logs with their respective databases

        3. JVM crash logs (hs_err_pid*.log):
           - Default: ./log/hs_err_pid*.log
           - Customize with ARCADEDB_JVM_ERROR_FILE before importing

        To customize locations, set environment variables BEFORE importing:
            os.environ["ARCADEDB_JVM_ERROR_FILE"] = "/custom/path/errors.log"
            import arcadedb_embedded as arcadedb

        Note: Application log location is hardcoded in ArcadeDB's Java code
        and cannot be changed via environment variables.
    """
    # Normalize to absolute path to avoid path doubling issues
    abs_root_path = os.path.abspath(root_path)

    return ArcadeDBServer(abs_root_path, root_password, config)
