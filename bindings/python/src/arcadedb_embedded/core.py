"""
ArcadeDB Python Bindings - Core Database Classes

Database and DatabaseFactory classes for embedded database access.
"""

from typing import List, Optional

from .exceptions import ArcadeDBError
from .jvm import start_jvm
from .results import ResultSet
from .transactions import TransactionContext
from .vector import VectorIndex, to_java_float_array


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
        max_items: int = 10000,
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
        start_jvm()

        from com.arcadedb.index.vector import HnswVectorIndexRAM
        from com.arcadedb.schema import Type
        from com.github.jelmerk.knn import DistanceFunctions

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
            raise ArcadeDBError(f"Failed to create vector index: {e}") from e

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
