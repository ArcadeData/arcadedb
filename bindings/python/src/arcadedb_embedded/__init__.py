"""
ArcadeDB Python Embedded Bindings

A native Python bindings for ArcadeDB that embeds the Java database engine
directly in the Python process using JPype.
"""

# The installed distribution's version is the single source of truth: the
# wheel is versioned from the release tag, while the generated _version.py
# is derived from the Maven pom, so the two disagree on tagged releases
# (a 26.8.1.dev20 wheel carried __version__ == "26.8.1.dev0"). Fall back to
# the generated file, then to a placeholder, for source checkouts that were
# never installed.
try:
    from importlib.metadata import version as _dist_version

    __version__ = _dist_version("arcadedb-embedded")
except Exception:  # not installed: fall back to the build-time file
    try:
        from ._version import __version__
    except ModuleNotFoundError:
        __version__ = "0.0.0"

# Import async execution
from .async_executor import AsyncExecutor

# Import citation helper
from .citation import cite

# Import core database classes
from .core import (
    Database,
    DatabaseFactory,
    create_database,
    database_exists,
    open_database,
)

# Import exceptions
from .exceptions import ArcadeDBError

# Import exporter classes
from .exporter import export_database, export_to_csv

# Import graph classes
from .graph import Document, Edge, Vertex

# Import graph batch helper
from .graph_batch import GraphBatch

# Import importer helpers
from .importer import ImportResult

# Which engine this install actually carries. __version__ is the PACKAGE
# version and can disagree with the bundled JARs without any error.
from .jvm import jar_fingerprint

# Import result classes
from .results import Result, ResultSet

# Import schema classes
from .schema import IndexType, PropertyType, Schema

# Import server classes
from .server import ArcadeDBServer, create_server

# Import transaction management
from .transactions import TransactionContext

# Import type conversion utilities
from .type_conversion import convert_java_to_python, convert_python_to_java

# Import vector utilities and index
from .vector import (
    VectorIndex,
    to_java_byte_array,
    to_java_float_array,
    to_python_array,
)

__all__ = [
    "__version__",
    # Exceptions
    "ArcadeDBError",
    # Core classes
    "Database",
    "DatabaseFactory",
    # Record wrappers
    "Document",
    "Vertex",
    "Edge",
    # Citation
    "cite",
    # Build provenance
    "jar_fingerprint",
    "create_database",
    "open_database",
    "database_exists",
    # Result classes
    "ResultSet",
    "Result",
    # Schema classes
    "Schema",
    "IndexType",
    "PropertyType",
    # Transaction management
    "TransactionContext",
    # Async execution
    "AsyncExecutor",
    "GraphBatch",
    "ImportResult",
    # Type conversion
    "convert_java_to_python",
    "convert_python_to_java",
    # Vector search
    "VectorIndex",
    "to_java_byte_array",
    "to_java_float_array",
    "to_python_array",
    # Server classes
    "ArcadeDBServer",
    "create_server",
    # Data export
    "export_database",
    "export_to_csv",
]
