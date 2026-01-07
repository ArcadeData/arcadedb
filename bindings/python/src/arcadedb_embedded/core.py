"""
ArcadeDB Python Bindings - Core Database Classes

Database and DatabaseFactory classes for embedded database access.
"""

from typing import List, Optional

from .exceptions import ArcadeDBError
from .jvm import start_jvm
from .results import ResultSet
from .transactions import TransactionContext
from .vector import to_java_float_array


class Database:
    """ArcadeDB Database wrapper."""

    def __init__(self, java_database):
        self._java_db = java_database
        self._closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def query(self, language: str, command: str, *args) -> ResultSet:
        """Execute a query and return results."""
        self._check_not_closed()
        try:
            if args:
                java_result = self._java_db.query(language, command, *args)
            else:
                java_result = self._java_db.query(language, command)
            return ResultSet(java_result)
        except Exception as e:
            raise ArcadeDBError(f"Query failed: {e}") from e

    def command(self, language: str, command: str, *args) -> Optional[ResultSet]:
        """Execute a command (non-idempotent operation)."""
        self._check_not_closed()
        try:
            if args:
                java_result = self._java_db.command(language, command, *args)
            else:
                java_result = self._java_db.command(language, command)

            if java_result is not None:
                return ResultSet(java_result)
            return None
        except Exception as e:
            raise ArcadeDBError(f"Command failed: {e}") from e

    def begin(self):
        """Begin a transaction."""
        self._check_not_closed()
        try:
            self._java_db.begin()
        except Exception as e:
            raise ArcadeDBError(f"Failed to begin transaction: {e}") from e

    def commit(self):
        """Commit the current transaction."""
        self._check_not_closed()
        try:
            self._java_db.commit()
        except Exception as e:
            raise ArcadeDBError(f"Failed to commit transaction: {e}") from e

    def rollback(self):
        """Rollback the current transaction."""
        self._check_not_closed()
        try:
            self._java_db.rollback()
        except Exception as e:
            raise ArcadeDBError(f"Failed to rollback transaction: {e}") from e

    def transaction(self) -> TransactionContext:
        """Create a transaction context manager."""
        return TransactionContext(self)

    def new_vertex(self, type_name: str):
        """Create a new vertex."""
        self._check_not_closed()
        try:
            return self._java_db.newVertex(type_name)
        except Exception as e:
            raise ArcadeDBError(
                f"Failed to create vertex of type '{type_name}': {e}"
            ) from e

    def new_document(self, type_name: str):
        """Create a new document."""
        self._check_not_closed()
        try:
            return self._java_db.newDocument(type_name)
        except Exception as e:
            raise ArcadeDBError(
                f"Failed to create document of type '{type_name}': {e}"
            ) from e

    def close(self):
        """Close the database."""
        if not self._closed and self._java_db is not None:
            try:
                self._java_db.close()
            except Exception as e:
                # Server-managed databases cannot be closed directly
                # They are shared and managed by the server lifecycle
                error_msg = str(e)
                if "cannot be closed" in error_msg.lower():
                    # Silently ignore - this is expected for server databases
                    self._closed = True
                    return
                raise ArcadeDBError(f"Failed to close database: {e}") from e
            finally:
                self._closed = True

    def is_open(self) -> bool:
        """Check if database is open."""
        return not self._closed and self._java_db.isOpen()

    def get_name(self) -> str:
        """Get the database name."""
        self._check_not_closed()
        try:
            return self._java_db.getName()
        except Exception as e:
            raise ArcadeDBError(f"Failed to get database name: {e}") from e

    def get_database_path(self) -> str:
        """Get the database path."""
        self._check_not_closed()
        try:
            return self._java_db.getDatabasePath()
        except Exception as e:
            raise ArcadeDBError(f"Failed to get database path: {e}") from e

    def count_type(self, type_name: str) -> int:
        """
        Count records of a specific type.

        Args:
            type_name: Name of the type to count

        Returns:
            Number of records

        Example:
            >>> user_count = db.count_type("User")
            >>> print(f"Total users: {user_count}")
        """
        self._check_not_closed()
        try:
            return self._java_db.countType(type_name, True)  # polymorphic=True
        except Exception as e:
            # If type doesn't exist, return 0
            if "was not found" in str(e) or "SchemaException" in str(e):
                return 0
            raise ArcadeDBError(f"Failed to count type '{type_name}': {e}") from e

    def drop(self):
        """
        Drop the entire database.

        WARNING: This deletes all data permanently!

        Example:
            >>> db = arcade.open_database("/tmp/test_db")
            >>> db.drop()  # Database is deleted
        """
        self._check_not_closed()
        try:
            self._java_db.drop()
            self._closed = True
        except Exception as e:
            raise ArcadeDBError(f"Failed to drop database: {e}") from e

    def is_transaction_active(self) -> bool:
        """
        Check if a transaction is currently active.

        Returns:
            True if transaction is active, False otherwise

        Example:
            >>> with db.transaction():
            ...     print(db.is_transaction_active())  # True
            >>> print(db.is_transaction_active())  # False
        """
        self._check_not_closed()
        try:
            return self._java_db.isTransactionActive()
        except Exception as e:
            raise ArcadeDBError(f"Failed to check transaction status: {e}") from e

    def async_executor(self):
        """
        Get async executor for parallel operations.

        Returns async executor that enables:
        - Parallel record creation (3-5x faster bulk inserts)
        - Automatic transaction batching
        - Optimized WAL configuration
        - 50,000-200,000 records/sec throughput

        Returns:
            AsyncExecutor instance configured for this database

        Example:
            >>> # Configure async executor
            >>> async_exec = db.async_executor()
            >>> async_exec.set_parallel_level(8)  # 8 worker threads
            >>> async_exec.set_commit_every(5000)  # Auto-commit every 5K
            >>>
            >>> # Create 100K records in parallel
            >>> for i in range(100000):
            ...     vertex = db.new_vertex("User")
            ...     vertex.set("id", i)
            ...     async_exec.create_record(vertex)
            >>>
            >>> # Wait for completion
            >>> async_exec.wait_completion()

        Note:
            The async executor is most beneficial for bulk operations.
            For small batches (<1000 records), regular transactions
            may be simpler and sufficient.
        """
        self._check_not_closed()
        from .async_executor import AsyncExecutor

        # JPype converts 'async' to 'async_' to avoid Python keyword collision
        return AsyncExecutor(self._java_db.async_())

    def batch_context(
        self,
        batch_size: int = 5000,
        parallel: int = 4,
        use_wal: bool = True,
        back_pressure: int = 0,
        progress: bool = False,
        progress_desc: str = "Processing",
    ):
        """
        Create a batch processing context manager.

        Provides a high-level interface for bulk operations with automatic
        async executor configuration, progress tracking, and error handling.

        Args:
            batch_size: Auto-commit every N operations (default: 5000)
            parallel: Number of parallel worker threads 1-16 (default: 4)
            use_wal: Enable Write-Ahead Log (default: True)
            back_pressure: Queue back-pressure threshold 0-100 (default: 0)
            progress: Enable progress tracking (default: False)
            progress_desc: Description for progress bar (default: "Processing")

        Returns:
            BatchContext: Context manager for batch operations

        Example:
            >>> # Simple batch processing
            >>> with db.batch_context(batch_size=10000, parallel=8) as batch:
            ...     for record in large_dataset:
            ...         batch.create_vertex("User", name=record['name'], age=record['age'])
            >>> # Auto-commits and waits for completion on exit

        Example with progress:
            >>> # With progress tracking
            >>> with db.batch_context(batch_size=5000, progress=True) as batch:
            ...     batch.set_total(len(dataset))
            ...     for record in dataset:
            ...         batch.create_vertex("User", **record)
            >>> # Prints progress bar (requires tqdm package)

        Example with error handling:
            >>> # Collect and check errors
            >>> with db.batch_context(batch_size=5000) as batch:
            ...     for record in dataset:
            ...         batch.create_document("LogEntry", **record)
            >>> if batch.get_errors():
            ...     print(f"Encountered {len(batch.get_errors())} errors")

        Note:
            This is a convenience wrapper around async_executor() that:
            - Automatically configures the async executor
            - Waits for completion on context exit
            - Provides simple create_vertex/document/edge methods
            - Optionally tracks progress with tqdm
        """
        self._check_not_closed()
        from .batch import BatchContext

        return BatchContext(
            self,
            batch_size=batch_size,
            parallel=parallel,
            use_wal=use_wal,
            back_pressure=back_pressure,
            progress=progress,
            progress_desc=progress_desc,
        )

    def export_database(
        self,
        file_path: str,
        format: str = "jsonl",
        overwrite: bool = False,
        include_types: Optional[List[str]] = None,
        exclude_types: Optional[List[str]] = None,
        verbose: int = 1,
    ) -> dict:
        """
        Export database to file.

        Supports JSONL (recommended for backup/restore), GraphML (graph visualization),
        and GraphSON (TinkerPop compatibility) formats.

        Args:
            file_path: Output file path
            format: Export format - "jsonl", "graphml", or "graphson"
            overwrite: Overwrite existing file if True
            include_types: List of types to export (None = all)
            exclude_types: List of types to exclude (None = none)
            verbose: Logging verbosity (0-2)

        Returns:
            Dictionary with export statistics (totalRecords, vertices, edges, etc.)

        Example:
            >>> # Export entire database to JSONL
            >>> stats = db.export_database("backup.jsonl.tgz", overwrite=True)
            >>> print(f"Exported {stats['totalRecords']} records")

            >>> # Export to GraphML for visualization
            >>> db.export_database("graph.graphml.tgz", format="graphml")

            >>> # Export specific types only
            >>> db.export_database(
            ...     "movies.jsonl.tgz",
            ...     include_types=["Movie", "Rating"]
            ... )
        """
        self._check_not_closed()
        from .exporter import export_database

        return export_database(
            self, file_path, format, overwrite, include_types, exclude_types, verbose
        )

    def export_to_csv(
        self,
        query: str,
        file_path: str,
        language: str = "sql",
        fieldnames: Optional[List[str]] = None,
    ):
        """
        Export query results to CSV file.

        Convenience method that executes query and exports results.

        Args:
            query: SQL query to execute
            file_path: Output CSV file path
            language: Query language (default: "sql")
            fieldnames: Column names (auto-detected if None)

        Example:
            >>> # Export all movies to CSV
            >>> db.export_to_csv("SELECT * FROM Movie", "movies.csv")

            >>> # Export with specific columns
            >>> db.export_to_csv(
            ...     "SELECT userId, movieId, rating FROM Rating WHERE rating >= 4.5",
            ...     "high_ratings.csv",
            ...     fieldnames=["user", "movie", "score"]
            ... )
        """
        self._check_not_closed()
        results = self.query(language, query)
        from .exporter import export_to_csv

        export_to_csv(results, file_path, fieldnames)

    def _check_not_closed(self):
        """Check if database is still open."""
        if self._closed:
            raise ArcadeDBError("Database is closed")

    def __del__(self):
        """Finalizer - ensure database is closed when object is garbage collected."""
        try:
            if not self._closed and self._java_db is not None:
                self._java_db.close()
                self._closed = True
        except Exception:
            pass  # Ignore errors during garbage collection


class DatabaseFactory:
    """Factory for creating/opening ArcadeDB databases."""

    def __init__(self, path: str):
        start_jvm()
        from com.arcadedb.database import DatabaseFactory as JavaDatabaseFactory

        self._java_factory = JavaDatabaseFactory(path)

    def create(self) -> Database:
        """Create a new database."""
        try:
            java_db = self._java_factory.create()
            return Database(java_db)
        except Exception as e:
            raise ArcadeDBError(f"Failed to create database: {e}") from e

    def open(self) -> Database:
        """Open an existing database."""
        try:
            java_db = self._java_factory.open()
            return Database(java_db)
        except Exception as e:
            raise ArcadeDBError(f"Failed to open database: {e}") from e

    def exists(self) -> bool:
        """Check if database exists."""
        try:
            return self._java_factory.exists()
        except Exception as e:
            raise ArcadeDBError(f"Failed to check if database exists: {e}") from e


# Convenience functions
def create_database(path: str) -> Database:
    """Create a new database at the given path."""
    factory = DatabaseFactory(path)
    return factory.create()


def open_database(path: str) -> Database:
    """Open an existing database at the given path."""
    factory = DatabaseFactory(path)
    return factory.open()


def database_exists(path: str) -> bool:
    """Check if a database exists at the given path."""
    factory = DatabaseFactory(path)
    return factory.exists()
