"""
ArcadeDB Python Embedded Driver

A native Python driver for ArcadeDB that embeds the Java database engine
directly in the Python process using JPype.
"""

import os
import glob
from pathlib import Path
from typing import Optional, Dict, Any, List
import jpype
import jpype.imports

# Import version from generated _version.py file (created during build)
from ._version import __version__

__all__ = [
    "__version__",
    "Database",
    "DatabaseFactory",
    "ArcadeDBServer",
    "create_database",
    "open_database",
    "create_server",
    "ArcadeDBError",
    "TransactionContext",
    "VectorIndex",
    "to_java_float_array",
    "to_python_array"
]


class ArcadeDBError(Exception):
    """Base exception for ArcadeDB errors."""
    pass


# ============================================================================
# Vector Operations Utilities
# ============================================================================

def to_java_float_array(vector):
    """
    Convert a Python array-like object to a Java float array.
    
    Accepts:
    - Python lists: [0.1, 0.2, 0.3]
    - NumPy arrays: np.array([0.1, 0.2, 0.3])
    - Any array-like object with __iter__
    
    Args:
        vector: Array-like object containing float values
        
    Returns:
        Java float array compatible with ArcadeDB HNSW indexes
    """
    import jpype.types as jtypes
    
    # Handle NumPy arrays
    try:
        import numpy as np
        if isinstance(vector, np.ndarray):
            vector = vector.tolist()
    except ImportError:
        pass
    
    # Convert to Python list if needed
    if not isinstance(vector, list):
        vector = list(vector)
    
    # Create Java float array
    return jtypes.JArray(jtypes.JFloat)(vector)


def to_python_array(java_vector, use_numpy=True):
    """
    Convert a Java array or ArrayList to a Python array.
    
    Args:
        java_vector: Java array or ArrayList of floats
        use_numpy: If True and NumPy is available, return np.array.
                   If False or NumPy unavailable, return Python list.
    
    Returns:
        NumPy array if use_numpy=True and NumPy is available, else Python list
    """
    # Convert to Python list first
    if hasattr(java_vector, '__iter__'):
        py_list = list(java_vector)
    else:
        py_list = [java_vector]
    
    # Try to return NumPy array if requested
    if use_numpy:
        try:
            import numpy as np
            return np.array(py_list, dtype=np.float32)
        except ImportError:
            pass
    
    return py_list


class TransactionContext:
    """Context manager for ArcadeDB transactions."""
    
    def __init__(self, database):
        self.database = database
        self.started = False
    
    def __enter__(self):
        self.database.begin()
        self.started = True
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.started:
            if exc_type is None:
                self.database.commit()
            else:
                self.database.rollback()


class VectorIndex:
    """
    Wrapper for ArcadeDB HNSW vector index.
    
    Provides a Pythonic interface for creating and searching vector indexes,
    with native support for NumPy arrays.
    """
    
    def __init__(self, java_index, database):
        """
        Initialize VectorIndex wrapper.
        
        Args:
            java_index: Java HnswVectorIndex object
            database: Parent Database object
        """
        self._java_index = java_index
        self._database = database
    
    def find_nearest(self, query_vector, k=10, use_numpy=True):
        """
        Find k nearest neighbors to the query vector.
        
        Args:
            query_vector: Query vector as Python list, NumPy array, or array-like
            k: Number of nearest neighbors to return (default: 10)
            use_numpy: Return vectors as NumPy arrays if available (default: True)
        
        Returns:
            List of tuples: [(vertex, distance), ...]
            where vertex is the matched vertex object and distance is the
            similarity score
        """
        # Convert query vector to Java float array
        java_vector = to_java_float_array(query_vector)
        
        # Perform search (None = no filtering callback)
        results = self._java_index.findNearest(java_vector, k, None)
        
        # Convert results to Python format
        neighbors = []
        for result in results:
            vertex = result.item()
            distance = result.distance()
            neighbors.append((vertex, float(distance)))
        
        return neighbors
    
    def add_vertex(self, vertex):
        """
        Add a single vertex to the index.
        
        Args:
            vertex: Vertex object to add (must have vector property)
        """
        try:
            self._java_index.add(vertex)
        except Exception as e:
            raise ArcadeDBError(f"Failed to add vertex to index: {e}")
    
    def remove_vertex(self, vertex_id):
        """
        Remove a vertex from the index.
        
        Args:
            vertex_id: ID of the vertex to remove
        """
        try:
            self._java_index.remove(vertex_id)
        except Exception as e:
            raise ArcadeDBError(f"Failed to remove vertex from index: {e}")


def _get_jar_path() -> str:
    """Get the path to bundled JAR files."""
    package_dir = Path(__file__).parent
    jar_dir = package_dir / "jars"
    return str(jar_dir)


def _start_jvm():
    """Start the JVM with ArcadeDB JARs if not already started."""
    if jpype.isJVMStarted():
        return
    
    jar_path = _get_jar_path()
    jar_files = glob.glob(os.path.join(jar_path, "*.jar"))
    
    if not jar_files:
        raise ArcadeDBError(
            f"No JAR files found in {jar_path}. "
            "The package may be corrupted or incomplete."
        )
    
    classpath = os.pathsep.join(jar_files)
    
    # Allow customization via environment variables
    max_heap = os.environ.get("ARCADEDB_JVM_MAX_HEAP", "2g")
    
    # Prepare JVM arguments
    jvm_args = [
        f"-Xmx{max_heap}",  # Max heap (default 2g, override with env var)
        "-Djava.awt.headless=true",  # Headless mode for server use
    ]
    
    # Configure JVM error log location (hs_err_pid*.log files)
    # By default these go to the current working directory
    error_file = os.environ.get("ARCADEDB_JVM_ERROR_FILE")
    if error_file:
        jvm_args.append(f"-XX:ErrorFile={error_file}")
    
    # Note: ArcadeDB application logs (arcadedb.log.*) are controlled by
    # the Java logging configuration in arcadedb-log.properties, which
    # defaults to ./log/arcadedb.log relative to the current working directory.
    # To change the log location:
    # 1. Set java.util.logging.config.file system property, OR
    # 2. Change the working directory before importing arcadedb_embedded, OR
    # 3. Create a custom arcadedb-log.properties in config/ directory
    
    try:
        jpype.startJVM(*jvm_args, classpath=classpath)
    except Exception as e:
        raise ArcadeDBError(f"Failed to start JVM: {e}")


class ResultSet:
    """Wrapper for ArcadeDB ResultSet."""
    
    def __init__(self, java_resultset):
        self._java_resultset = java_resultset
    
    def __iter__(self):
        return self
    
    def __next__(self):
        if self._java_resultset.hasNext():
            return Result(self._java_resultset.next())
        else:
            raise StopIteration
    
    def has_next(self) -> bool:
        """Check if there are more results."""
        return self._java_resultset.hasNext()
    
    def next(self) -> 'Result':
        """Get the next result."""
        if self.has_next():
            return Result(self._java_resultset.next())
        raise StopIteration
    
    def close(self):
        """Close the result set."""
        if hasattr(self._java_resultset, 'close'):
            self._java_resultset.close()


class Result:
    """Wrapper for ArcadeDB Result."""
    
    def __init__(self, java_result):
        self._java_result = java_result
    
    def get_property(self, name: str) -> Any:
        """Get a property value."""
        try:
            return self._java_result.getProperty(name)
        except Exception as e:
            raise ArcadeDBError(f"Failed to get property '{name}': {e}")
    
    def has_property(self, name: str) -> bool:
        """Check if property exists."""
        return self._java_result.hasProperty(name)
    
    def get_property_names(self) -> List[str]:
        """Get all property names."""
        return list(self._java_result.getPropertyNames())
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        result_dict = {}
        for prop_name in self.get_property_names():
            result_dict[prop_name] = self.get_property(prop_name)
        return result_dict
    
    def to_json(self) -> str:
        """Convert result to JSON string."""
        return str(self._java_result.toJSON())


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
            raise ArcadeDBError(f"Query failed: {e}")
    
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
            raise ArcadeDBError(f"Command failed: {e}")
    
    def begin(self):
        """Begin a transaction."""
        self._check_not_closed()
        try:
            self._java_db.begin()
        except Exception as e:
            raise ArcadeDBError(f"Failed to begin transaction: {e}")
    
    def commit(self):
        """Commit the current transaction."""
        self._check_not_closed()
        try:
            self._java_db.commit()
        except Exception as e:
            raise ArcadeDBError(f"Failed to commit transaction: {e}")
    
    def rollback(self):
        """Rollback the current transaction."""
        self._check_not_closed()
        try:
            self._java_db.rollback()
        except Exception as e:
            raise ArcadeDBError(f"Failed to rollback transaction: {e}")
    
    def transaction(self) -> TransactionContext:
        """Create a transaction context manager."""
        return TransactionContext(self)
    
    def new_vertex(self, type_name: str):
        """Create a new vertex."""
        self._check_not_closed()
        try:
            return self._java_db.newVertex(type_name)
        except Exception as e:
            raise ArcadeDBError(f"Failed to create vertex of type '{type_name}': {e}")
    
    def new_document(self, type_name: str):
        """Create a new document."""
        self._check_not_closed()
        try:
            return self._java_db.newDocument(type_name)
        except Exception as e:
            raise ArcadeDBError(f"Failed to create document of type '{type_name}': {e}")
    
    def close(self):
        """Close the database."""
        if not self._closed and self._java_db is not None:
            try:
                self._java_db.close()
            except Exception as e:
                raise ArcadeDBError(f"Failed to close database: {e}")
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
            raise ArcadeDBError(f"Failed to get database name: {e}")
    
    def get_database_path(self) -> str:
        """Get the database path."""
        self._check_not_closed()
        try:
            return self._java_db.getDatabasePath()
        except Exception as e:
            raise ArcadeDBError(f"Failed to get database path: {e}")
    
    def create_vector_index(
        self,
        vertex_type: str,
        vector_property: str,
        dimensions: int,
        id_property: str = "id",
        edge_type: str = "VectorProximity",
        deleted_property: str = "deleted",
        distance_function: str = "cosine",
        m: int = 16,
        ef: int = 128,
        ef_construction: int = 128,
        max_items: int = 10000
    ) -> VectorIndex:
        """
        Create an HNSW vector index for similarity search.
        
        This is a high-level convenience method that handles the complex
        Java API for creating HNSW indexes.
        
        Args:
            vertex_type: Name of the vertex type containing vectors
            vector_property: Name of the property storing vector arrays
            dimensions: Dimensionality of the vectors
            id_property: Property to use as unique ID (default: "id")
            edge_type: Edge type for proximity graph (default: "VectorProximity")
            deleted_property: Property marking deleted items (default: "deleted")
            distance_function: Distance metric - "cosine", "euclidean", or
                               "inner_product" (default: "cosine")
            m: HNSW M parameter - number of bi-directional links per node
               (default: 16)
            ef: HNSW ef parameter - size of dynamic candidate list for search
                (default: 128)
            ef_construction: Size of dynamic candidate list during construction
                             (default: 128)
            max_items: Maximum number of items in the index (default: 10000)
        
        Returns:
            VectorIndex object for performing similarity searches
        
        Example:
            >>> import numpy as np
            >>> with db.transaction():
            ...     db.command("sql", "CREATE VERTEX TYPE Doc")
            ...     db.command("sql",
            ...                "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS")
            ...     db.command("sql", "CREATE PROPERTY Doc.id STRING")
            ...
            >>> index = db.create_vector_index("Doc", "embedding", dimensions=384)
            >>>
            >>> # Add vectors (as NumPy arrays or Python lists)
            >>> with db.transaction():
            ...     for i, emb in enumerate(embeddings):
            ...         vertex = db.new_vertex("Doc")
            ...         vertex.set("id", f"doc_{i}")
            ...         vertex.set("embedding", to_java_float_array(emb))
            ...         vertex.save()
            ...         index.add_vertex(vertex)
            ...
            >>> # Search
            >>> query_vector = np.random.rand(384)
            >>> neighbors = index.find_nearest(query_vector, k=5)
        """
        self._check_not_closed()
        _start_jvm()
        
        from com.arcadedb.index.vector import HnswVectorIndexRAM
        from com.github.jelmerk.knn import DistanceFunctions
        from com.arcadedb.schema import Type
        
        # Map distance function names to Java constants
        distance_funcs = {
            "cosine": DistanceFunctions.FLOAT_COSINE_DISTANCE,
            "euclidean": DistanceFunctions.FLOAT_EUCLIDEAN_DISTANCE,
            "inner_product": DistanceFunctions.FLOAT_INNER_PRODUCT,
        }
        
        if distance_function not in distance_funcs:
            raise ArcadeDBError(
                f"Invalid distance function: {distance_function}. "
                f"Must be one of: {', '.join(distance_funcs.keys())}"
            )
        
        distance_func = distance_funcs[distance_function]
        
        try:
            # Build HNSW RAM index
            builder = HnswVectorIndexRAM.newBuilder(
                dimensions, distance_func, max_items
            )
            builder = builder.withM(m)
            builder = builder.withEf(ef)
            builder = builder.withEfConstruction(ef_construction)
            
            hnsw_ram = builder.build()
            
            # Create persistent index
            persistent_builder = hnsw_ram.createPersistentIndex(self._java_db)
            persistent_builder = persistent_builder.withVertexType(vertex_type)
            persistent_builder = persistent_builder.withEdgeType(edge_type)
            persistent_builder = persistent_builder.withVectorProperty(
                vector_property, Type.ARRAY_OF_FLOATS
            )
            persistent_builder = persistent_builder.withIdProperty(id_property)
            persistent_builder = persistent_builder.withDeletedProperty(
                deleted_property
            )
            
            # Create the index
            java_index = persistent_builder.create()
            
            return VectorIndex(java_index, self)
            
        except Exception as e:
            raise ArcadeDBError(f"Failed to create vector index: {e}")
    
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
        _start_jvm()
        from com.arcadedb.database import DatabaseFactory as JavaDatabaseFactory
        self._java_factory = JavaDatabaseFactory(path)
    
    def create(self) -> Database:
        """Create a new database."""
        try:
            java_db = self._java_factory.create()
            return Database(java_db)
        except Exception as e:
            raise ArcadeDBError(f"Failed to create database: {e}")
    
    def open(self) -> Database:
        """Open an existing database."""
        try:
            java_db = self._java_factory.open()
            return Database(java_db)
        except Exception as e:
            raise ArcadeDBError(f"Failed to open database: {e}")
    
    def exists(self) -> bool:
        """Check if database exists."""
        try:
            return self._java_factory.exists()
        except Exception as e:
            raise ArcadeDBError(f"Failed to check if database exists: {e}")


class ArcadeDBServer:
    """ArcadeDB Server wrapper for enabling HTTP API and Studio access."""
    
    def __init__(
        self,
        root_path: str = "./databases",
        root_password: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None
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
        _start_jvm()
        from com.arcadedb.server import ArcadeDBServer as JavaArcadeDBServer
        from com.arcadedb import ContextConfiguration
        
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
            context_config.setValue(
                "arcadedb.server.rootPassword", root_password
            )
        
        # Set common defaults
        mode = self._config.get("mode", "development")
        context_config.setValue("arcadedb.server.mode", mode)
        
        host = self._config.get("host", "0.0.0.0")
        context_config.setValue("arcadedb.server.httpIncoming.host", host)
        
        http_port = self._config.get("http_port", 2480)
        context_config.setValue(
            "arcadedb.server.httpIncoming.port", http_port
        )
        
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
            raise ArcadeDBError(f"Failed to start server: {e}")
    
    def stop(self):
        """Stop the ArcadeDB server."""
        if not self._started:
            return
        
        try:
            self._java_server.stop()
            self._started = False
        except Exception as e:
            raise ArcadeDBError(f"Failed to stop server: {e}")
    
    def __del__(self):
        """Finalizer - ensure server is stopped when object is garbage collected."""
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
            raise ArcadeDBError(f"Failed to get database '{name}': {e}")
    
    def create_database(self, name: str) -> Database:
        """
        Create a new database on the server.
        
        Args:
            name: Database name
            
        Returns:
            Database instance
        """
        if not self._started:
            raise ArcadeDBError("Server is not started")
        
        try:
            # Use DatabaseFactory to create the database
            db_path = os.path.join(self._root_path, name)
            factory = DatabaseFactory(db_path)
            return factory.create()
        except Exception as e:
            raise ArcadeDBError(f"Failed to create database '{name}': {e}")
    
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


def create_server(
    root_path: str = "./databases",
    root_password: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None
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
    """
    return ArcadeDBServer(root_path, root_password, config)


# Shutdown hook to clean up JVM
# Note: JVM shutdown is disabled because it conflicts with ArcadeDB's
# internal shutdown hooks and causes hangs. The JVM will be cleaned up
# by the OS when the process exits.


def _shutdown_jvm():
    """Shutdown JVM if it was started by this module."""
    if jpype.isJVMStarted():
        try:
            jpype.shutdownJVM()
        except Exception:
            pass  # Ignore errors during shutdown


# Disabled - causes hangs with ArcadeDB server shutdown hooks
# import atexit
# atexit.register(_shutdown_jvm)
