"""
ArcadeDB Python Embedded Bindings

A native Python bindings for ArcadeDB that embeds the Java database engine
directly in the Python process using JPype.
"""

# Import version from generated _version.py file (created during build)
from ._version import __version__

# Import async execution
from .async_executor import AsyncExecutor

# Import batch processing
from .batch import BatchContext

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

# Import importer classes
from .importer import Importer, import_csv, import_json, import_neo4j

# Import result classes
from .results import Result, ResultSet

# Import server classes
from .server import ArcadeDBServer, create_server

# Import transaction management
from .transactions import TransactionContext

# Import type conversion utilities
from .type_conversion import convert_java_to_python, convert_python_to_java

# Import vector utilities and index
from .vector import VectorIndex, to_java_float_array, to_python_array

__all__ = [
    "__version__",
    # Exceptions
    "ArcadeDBError",
    # Core classes
    "Database",
    "DatabaseFactory",
    "create_database",
    "open_database",
    "database_exists",
    # Server classes
    "ArcadeDBServer",
    "create_server",
    # Result classes
    "ResultSet",
    "Result",
    # Transaction management
    "TransactionContext",
    # Async execution
    "AsyncExecutor",
    # Batch processing
    "BatchContext",
    # Type conversion
    "convert_java_to_python",
    "convert_python_to_java",
    # Vector search
    "VectorIndex",
    "to_java_float_array",
    "to_python_array",
    # Data export
    "export_database",
    "export_to_csv",
    # Data import
    "Importer",
    "import_json",
    "import_csv",
    "import_neo4j",
]
