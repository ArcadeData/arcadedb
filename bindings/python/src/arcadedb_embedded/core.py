"""
ArcadeDB Python Bindings - Core Database Classes

Database and DatabaseFactory classes for embedded database access.
"""

from typing import Any, List, Optional

from .exceptions import ArcadeDBError
from .graph import Document, Edge, Vertex
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
                # Convert NumPy arrays to Java float arrays
                converted_args = []
                for arg in args:
                    if (
                        hasattr(arg, "__class__")
                        and arg.__class__.__name__ == "ndarray"
                    ):
                        converted_args.append(to_java_float_array(arg))
                    else:
                        converted_args.append(arg)
                java_result = self._java_db.query(language, command, *converted_args)
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
                # Convert NumPy arrays to Java float arrays
                converted_args = []
                for arg in args:
                    if (
                        hasattr(arg, "__class__")
                        and arg.__class__.__name__ == "ndarray"
                    ):
                        converted_args.append(to_java_float_array(arg))
                    else:
                        converted_args.append(arg)
                java_result = self._java_db.command(language, command, *converted_args)
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

    def new_vertex(self, type_name: str) -> Vertex:
        """Create a new vertex."""
        self._check_not_closed()
        try:
            return Vertex(self._java_db.newVertex(type_name))
        except Exception as e:
            raise ArcadeDBError(
                f"Failed to create vertex of type '{type_name}': {e}"
            ) from e

    def new_document(self, type_name: str) -> Document:
        """Create a new document."""
        self._check_not_closed()
        try:
            return Document(self._java_db.newDocument(type_name))
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

    def lookup_by_key(self, type_name: str, keys: List[str], values: List[Any]):
        """
        Lookup records by indexed key (O(1) index-based lookup).

        Args:
            type_name: Type name
            keys: List of property names (must be indexed)
            values: List of property values

        Returns:
            Python-wrapped records or None

        Example:
            >>> records = list(db.lookup_by_key("User", ["email"], ["alice@example.com"]))
            >>> if records:
            ...     user = records[0]
        """
        self._check_not_closed()
        try:
            import jpype

            # Convert to Java arrays
            keys_array = jpype.JArray(jpype.JString)(keys)
            values_array = jpype.JArray(jpype.JObject)(values)

            cursor = self._java_db.lookupByKey(type_name, keys_array, values_array)

            # Return first result wrapped, or None
            if cursor.hasNext():
                java_record = cursor.next().getRecord()
                return Document.wrap(java_record)
            return None
        except Exception as e:
            raise ArcadeDBError(f"Failed to lookup by key in '{type_name}': {e}") from e

    def lookup_by_rid(self, rid: str) -> Any:
        """
        Lookup a record by its RID.

        Args:
            rid: Record ID string (e.g. "#10:5")

        Returns:
            Record object (Vertex, Document, or Edge) or None if not found

        Example:
            >>> record = db.lookup_by_rid("#10:5")
            >>> if record:
            ...     print(record.get("name"))
        """
        self._check_not_closed()
        try:
            import jpype
            from com.arcadedb.database import RID

            java_rid = RID(self._java_db, rid)
            java_record = self._java_db.lookupByRID(java_rid, True)

            if java_record is None:
                return None

            # Wrap in appropriate Python class
            if isinstance(java_record, jpype.JClass("com.arcadedb.graph.Vertex")):
                return Vertex(java_record)
            elif isinstance(java_record, jpype.JClass("com.arcadedb.graph.Edge")):
                return Edge(java_record)
            elif isinstance(
                java_record, jpype.JClass("com.arcadedb.database.Document")
            ):
                return Document(java_record)

            return java_record
        except Exception as e:
            raise ArcadeDBError(f"Failed to lookup RID '{rid}': {e}") from e

    def create_vector_index(
        self,
        vertex_type: str,
        vector_property: str,
        dimensions: int,
        distance_function: str = "cosine",
        max_connections: int = 16,
        beam_width: int = 100,
        quantization: str = "INT8",
        location_cache_size: Optional[int] = None,
        graph_build_cache_size: Optional[int] = None,
        mutations_before_rebuild: Optional[int] = None,
        store_vectors_in_graph: bool = False,
        add_hierarchy: Optional[bool] = True,
        pq_subspaces: Optional[int] = None,
        pq_clusters: Optional[int] = None,
        pq_center_globally: Optional[bool] = None,
        pq_training_limit: Optional[int] = None,
        build_graph_now: bool = True,
    ) -> "VectorIndex":
        """
        Create a vector index for similarity search (JVector implementation).

        This uses JVector (graph index combining HNSW hierarchy with Vamana/DiskANN)
        which provides:
        - No max_items limit (grows dynamically)
        - Fast index construction
        - Automatic indexing of existing records
        - Concurrent construction support

        Args:
            vertex_type: Name of the vertex type
            vector_property: Name of the property containing vectors
            dimensions: Vector dimensionality (e.g., 768 for BERT)
            distance_function: "cosine", "euclidean", or "inner_product"
            max_connections: Max connections per node (default: 16).
                             Maps to `maxConnections` in JVector.
            beam_width: Beam width for search/construction (default: 100).
                        Maps to `beamWidth` in JVector.
            quantization: Vector quantization type (default: INT8).
                          Options: "INT8", "BINARY", "PRODUCT" (PQ).
                          Reduces memory usage and speeds up search at the cost of
                          some precision. "PRODUCT" enables PQ data for
                          approximate search (zero-disk-I/O path).
            location_cache_size: Per-index override for vector location cache size
                (maps to Java metadata key "locationCacheSize"; uses
                GlobalConfiguration default if None). Typical ranges by corpus size
                (vectors):
                - ~100K: 50k–100k
                - ~1M: 100k–200k
                - ~10M: 300k–500k
                - ~100M: 500k–800k (scale with heap)
            graph_build_cache_size: Per-index override for graph build cache size
                (maps to Java metadata key "graphBuildCacheSize"; uses
                GlobalConfiguration default if None). Typical ranges (higher = faster
                build, more RAM):
                - ~100K: 10k–30k
                - ~1M: 30k–75k
                - ~10M: 75k–150k
                - ~100M: 150k–250k (only if heap allows)
            mutations_before_rebuild: Per-index override for mutations threshold
                before triggering a graph rebuild (maps to Java metadata key
                "mutationsBeforeRebuild"; uses GlobalConfiguration default if None).
                Typical ranges: 100–300 for freshness-heavy workloads; 300–800 for
                write-heavy workloads and larger graphs.
            pq_subspaces: Number of PQ subspaces (M). Requires quantization="PRODUCT".
            pq_clusters: Clusters per subspace (K). Requires quantization="PRODUCT".
            pq_center_globally: Whether to globally center vectors before PQ.
                Requires quantization="PRODUCT".
            pq_training_limit: Max vectors to use for PQ training. Requires
                quantization="PRODUCT".
            build_graph_now: If True (default), eagerly builds the vector graph
                immediately after index creation. If False, graph preparation is
                deferred and may happen lazily on first search.
            store_vectors_in_graph: Whether to store vectors inline in the graph
                structure (default: False). If True, increases disk usage but
                significantly speeds up search for large datasets by avoiding document
                lookups.
            add_hierarchy: Whether to build hierarchical layers in the HNSW graph
                (Default is True). If None, uses the engine default. Set explicitly to
                True/False to force the behavior.

        Returns:
            VectorIndex object
        """
        self._check_not_closed()
        from .schema import IndexType

        # Create the index using the Java Builder API directly to pass configuration
        try:
            import jpype

            if any(
                val is not None
                for val in (
                    pq_subspaces,
                    pq_clusters,
                    pq_center_globally,
                    pq_training_limit,
                )
            ):
                if not quantization or quantization.upper() != "PRODUCT":
                    raise ValueError("PQ parameters require quantization='PRODUCT'")

            java_schema = self.schema._java_schema

            # Convert property names to Java array
            java_props = jpype.JArray(jpype.JString)([vector_property])

            # Build the index
            builder = java_schema.buildTypeIndex(vertex_type, java_props)

            # Set type to LSM_VECTOR (this returns TypeLSMVectorIndexBuilder)
            INDEX_TYPE = jpype.JPackage("com").arcadedb.schema.Schema.INDEX_TYPE
            builder = builder.withType(INDEX_TYPE.LSM_VECTOR)

            # Configure
            builder.withDimensions(dimensions)
            builder.withSimilarity(distance_function)
            builder.withMaxConnections(max_connections)
            builder.withBeamWidth(beam_width)

            if quantization:
                builder.withQuantization(quantization)

            if pq_subspaces is not None:
                builder.withPQSubspaces(int(pq_subspaces))
            if pq_clusters is not None:
                builder.withPQClusters(int(pq_clusters))
            if pq_center_globally is not None:
                builder.withPQCenterGlobally(bool(pq_center_globally))
            if pq_training_limit is not None:
                builder.withPQTrainingLimit(int(pq_training_limit))

            metadata_cfg = {}
            if store_vectors_in_graph:
                metadata_cfg["storeVectorsInGraph"] = True
            if add_hierarchy is not None:
                metadata_cfg["addHierarchy"] = bool(add_hierarchy)
            if location_cache_size is not None:
                metadata_cfg["locationCacheSize"] = int(location_cache_size)
            if graph_build_cache_size is not None:
                metadata_cfg["graphBuildCacheSize"] = int(graph_build_cache_size)
            if mutations_before_rebuild is not None:
                metadata_cfg["mutationsBeforeRebuild"] = int(mutations_before_rebuild)

            if metadata_cfg:
                # Use JSON configuration to avoid JPype overload ambiguity on put()
                import json

                JSONObject = jpype.JPackage("com").arcadedb.serializer.json.JSONObject
                json_cfg = JSONObject(json.dumps(metadata_cfg))
                builder.withMetadata(json_cfg)

            # Create
            java_index = builder.create()

            from .vector import VectorIndex

            index = VectorIndex(java_index, self)
            if build_graph_now:
                index.build_graph_now()

            return index
        except Exception as e:
            raise ArcadeDBError(f"Failed to create vector index: {e}") from e

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
            >>> db = arcade.open_database("./test_db")
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

    def set_wal_flush(self, mode: str):
        """
        Configure Write-Ahead Log (WAL) flush strategy.

        Controls how aggressively changes are flushed to disk. This affects the
        durability/performance trade-off for transactions.

        Args:
            mode: WAL flush mode, one of:
                - 'no': No flush, maximum performance (default)
                - 'yes_nometadata': Flush data but not metadata
                - 'yes_full': Flush everything, maximum durability

        Raises:
            ValueError: If mode is not valid

        Example:
            >>> db.set_wal_flush('yes_full')  # Maximum durability
            >>> db.set_wal_flush('no')  # Maximum performance
        """
        self._check_not_closed()
        import jpype

        valid_modes = {
            "no": "NO",
            "yes_nometadata": "YES_NOMETADATA",
            "yes_full": "YES_FULL",
        }
        if mode not in valid_modes:
            raise ValueError(
                f"Invalid WAL flush mode: {mode}. "
                f"Must be one of: {list(valid_modes.keys())}"
            )

        try:
            WALFile = jpype.JPackage("com").arcadedb.engine.WALFile
            flush_type = getattr(WALFile.FlushType, valid_modes[mode])
            self._java_db.setWALFlush(flush_type)
        except Exception as e:
            raise ArcadeDBError(f"Failed to set WAL flush mode: {e}") from e

    def set_read_your_writes(self, enabled: bool):
        """
        Enable or disable read-your-writes consistency.

        When enabled, uncommitted changes in the current transaction are visible
        in subsequent reads. Disabling can improve concurrency but may show stale data.

        Args:
            enabled: True to enable read-your-writes, False to disable

        Example:
            >>> db.set_read_your_writes(True)  # Default behavior
            >>> db.set_read_your_writes(False)  # Better concurrency
        """
        self._check_not_closed()
        try:
            self._java_db.setReadYourWrites(enabled)
        except Exception as e:
            raise ArcadeDBError(f"Failed to set read-your-writes: {e}") from e

    def set_auto_transaction(self, enabled: bool):
        """
        Enable or disable automatic transaction management.

        When enabled, ArcadeDB automatically begins a transaction for operations
        that require one. When disabled, you must manually call begin_transaction().

        Args:
            enabled: True to enable auto-transaction, False to disable

        Example:
            >>> db.set_auto_transaction(False)  # Manual transaction control
            >>> db.begin_transaction()
            >>> # ... do work ...
            >>> db.commit()
            >>> db.set_auto_transaction(True)  # Restore default
        """
        self._check_not_closed()
        try:
            self._java_db.setAutoTransaction(enabled)
        except Exception as e:
            raise ArcadeDBError(f"Failed to set auto-transaction: {e}") from e

    def async_executor(self):
        """
        Get async executor for parallel operations.

        Experimental: not advised for production use yet.

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

    @property
    def schema(self):
        """
        Get the schema manipulation API for this database.

        The schema API provides type-safe access to schema operations:
        - Type management (document, vertex, edge types)
        - Property management (create, drop properties)
        - Index management (create, drop indexes)

        Returns:
            Schema instance for this database

        Example:
            >>> # Create a vertex type with properties
            >>> db.schema.create_vertex_type("User")
            >>> db.schema.create_property("User", "name", PropertyType.STRING)
            >>> db.schema.create_property("User", "age", PropertyType.INTEGER)
            >>>
            >>> # Create an index
            >>> db.schema.create_index("User", ["name"], unique=True)
            >>>
            >>> # Create edge type
            >>> db.schema.create_edge_type("Follows")

        Note:
            Schema changes are immediately persisted and visible to all
            database connections. Schema modifications should be done
            carefully in production environments.
        """
        self._check_not_closed()
        from .schema import Schema

        return Schema(self._java_db.getSchema(), self)

    def batch_context(
        self,
        batch_size: int = 5000,
        parallel: int = 4,
        use_wal: bool = True,
        back_pressure: int = 50,
        progress: bool = False,
        progress_desc: str = "Processing",
    ):
        """
        Create a batch processing context manager.

        Experimental: not advised for production use yet.

        Provides a high-level interface for bulk operations with automatic
        async executor configuration, progress tracking, and error handling.

        Args:
            batch_size: Auto-commit every N operations (default: 5000)
            parallel: Number of parallel worker threads 1-16 (default: 4)
            use_wal: Enable Write-Ahead Log (default: True)
            back_pressure: Queue back-pressure threshold 0-100 (default: 50)
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

    def __init__(
        self,
        path: str,
        jvm_kwargs: Optional[dict] = None,
    ):
        """
        Args:
            path: Database path
            jvm_kwargs: Optional JVM args passed to start_jvm()
                Example: {"heap_size": "8g"}
        """
        start_jvm(**(jvm_kwargs or {}))
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
def create_database(
    path: str,
    jvm_kwargs: Optional[dict] = None,
) -> Database:
    """Create a new database at the given path.

    Args:
        path: Database path
        jvm_kwargs: Optional JVM args passed to start_jvm()
            Example: {"heap_size": "8g"}
    """
    factory = DatabaseFactory(
        path,
        jvm_kwargs=jvm_kwargs,
    )
    return factory.create()


def open_database(
    path: str,
    jvm_kwargs: Optional[dict] = None,
) -> Database:
    """Open an existing database at the given path.

    Args:
        path: Database path
        jvm_kwargs: Optional JVM args passed to start_jvm()
            Example: {"heap_size": "8g"}
    """
    factory = DatabaseFactory(
        path,
        jvm_kwargs=jvm_kwargs,
    )
    return factory.open()


def database_exists(path: str) -> bool:
    """Check if a database exists at the given path."""
    factory = DatabaseFactory(path)
    return factory.exists()
