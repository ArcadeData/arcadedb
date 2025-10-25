"""
ArcadeDB Python Bindings - Data Importer

Thin wrapper over ArcadeDB's Java data import functionality.
Supports importing from JSON, CSV, and Neo4j export formats.
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional

from .exceptions import ArcadeDBError
from .jvm import start_jvm


class Importer:
    """
    Thin wrapper for ArcadeDB's Java importer.

    This class provides a Pythonic interface to ArcadeDB's production-tested
    Java importer, which supports:
    - JSON files (single or multiple objects)
    - CSV/TSV files (documents, vertices, edges with FK resolution)
    - Neo4j JSONL exports

    The Java importer handles:
    - Streaming parsing for memory efficiency
    - Automatic type inference
    - Batch transactions for optimal performance
    - Foreign key resolution for edge imports
    - Parallel processing support
    """

    def __init__(self, database):
        """
        Initialize importer for a database.

        Args:
            database: Database instance to import data into
        """
        self.database = database
        self._java_db = database._java_db
        start_jvm()

        # Import Java classes
        from com.arcadedb.integration.importer import Importer as JavaImporter
        from com.arcadedb.integration.importer import ImporterSettings

        self._JavaImporter = JavaImporter
        self._ImporterSettings = ImporterSettings

    def import_file(
        self,
        file_path: str,
        format_type: Optional[str] = None,
        import_type: str = "documents",
        type_name: Optional[str] = None,
        **options,
    ) -> Dict[str, Any]:
        """
        Import data from a file using Java importer. Auto-detects format if not specified.

        Args:
            file_path: Path to the file to import
            format_type: Format type: 'json', 'csv', 'neo4j'
                        (auto-detected from extension if None)
            import_type: Type of import: 'documents', 'vertices', or 'edges' (default: 'documents')
            type_name: Target document/vertex/edge type name
            **options: Additional format-specific options passed to Java ImporterSettings:

                Common options:
                - commitEvery: Commit every N records (default: 5000)
                - parallel: Number of parallel threads (default: CPU count / 2)
                - verboseLevel: Logging level 0-3 (default: 2)
                - trimText: Trim whitespace from text values (default: True)

                CSV options:
                - delimiter: Field delimiter (e.g., ',', '\t')
                - header: Has header row (default: True)
                - skipEntries: Number of entries to skip
                - propertiesInclude: Properties to include (default: '*')

                Edge-specific options (when import_type='edges'):
                - edgeFromField: Field containing source vertex ID/RID
                - edgeToField: Field containing target vertex ID/RID
                - edgeBidirectional: Create bidirectional edges (default: True)
                - typeIdProperty: Property to use as unique vertex ID
                - typeIdType: Type of ID property ('String', 'Long', etc.)

                Advanced options:
                - analysisLimitBytes: Bytes to analyze for schema (default: 100000)
                - analysisLimitEntries: Entries to analyze for schema (default: 10000)
                - maxRAMIncomingEdges: Max RAM for edge resolution (default: 256MB)

        Returns:
            Dict with import statistics:
            {
                'documents': count,
                'vertices': count,
                'edges': count,
                'errors': count,
                'duration_ms': milliseconds
            }

        Example:
            >>> # Import CSV as documents
            >>> importer = Importer(db)
            >>> stats = importer.import_file('data.csv', type_name='Person')
            >>>
            >>> # Import CSV as vertices
            >>> stats = importer.import_file('users.csv',
            ...                              import_type='vertices',
            ...                              type_name='User')
            >>>
            >>> # Import CSV as edges with FK resolution
            >>> stats = importer.import_file('follows.csv',
            ...                              import_type='edges',
            ...                              type_name='Follows',
            ...                              edgeFromField='user_id',
            ...                              edgeToField='follows_id',
            ...                              typeIdProperty='id')
        """
        if not os.path.exists(file_path):
            raise ArcadeDBError(f"File not found: {file_path}")

        # Auto-detect format from extension if not specified
        if format_type is None:
            ext = Path(file_path).suffix.lower()
            format_map = {
                ".json": "json",
                ".csv": "csv",
                ".tsv": "csv",
            }
            format_type = format_map.get(ext)
            if format_type is None:
                raise ArcadeDBError(
                    f"Cannot auto-detect format for extension: {ext}. "
                    "Please specify format_type explicitly."
                )

        # Validate import_type
        if import_type not in ("documents", "vertices", "edges"):
            raise ArcadeDBError(
                f"Invalid import_type: {import_type}. "
                "Must be 'documents', 'vertices', or 'edges'"
            )

        # Route to appropriate importer
        format_type = format_type.lower()
        if format_type in ("json", "csv"):
            return self._import_using_java(
                file_path, format_type, import_type, type_name, **options
            )
        elif format_type == "neo4j":
            return self._import_neo4j(file_path, **options)
        else:
            raise ArcadeDBError(
                f"Unsupported format: {format_type}. "
                "Supported formats: json, csv, neo4j"
            )

    def _import_using_java(
        self,
        file_path: str,
        format_type: str,
        import_type: str,
        type_name: Optional[str],
        **options,
    ) -> Dict[str, Any]:
        """
        Import using Java Importer with ImporterSettings.

        This is a thin wrapper that configures Java's production importer
        with all the settings and delegates the actual import work to Java.
        """
        try:
            # Create settings
            settings = self._ImporterSettings()

            # Set database path
            settings.database = self._java_db.getDatabasePath()

            # Set URL (file path)
            abs_path = os.path.abspath(file_path)
            settings.url = f"file://{abs_path}"

            # Configure based on import type
            if import_type == "documents":
                settings.documents = abs_path
                if type_name:
                    settings.documentTypeName = type_name
                if "delimiter" in options:
                    settings.documentsDelimiter = options["delimiter"]
                if "header" in options:
                    settings.documentsHeader = str(options["header"]).lower()
                if "skipEntries" in options:
                    settings.documentsSkipEntries = int(options["skipEntries"])
                if "propertiesInclude" in options:
                    settings.documentPropertiesInclude = options["propertiesInclude"]

            elif import_type == "vertices":
                settings.vertices = abs_path
                if type_name:
                    settings.vertexTypeName = type_name
                if "delimiter" in options:
                    settings.verticesDelimiter = options["delimiter"]
                if "header" in options:
                    settings.verticesHeader = str(options["header"]).lower()
                if "skipEntries" in options:
                    settings.verticesSkipEntries = int(options["skipEntries"])
                if "propertiesInclude" in options:
                    settings.vertexPropertiesInclude = options["propertiesInclude"]
                if "expectedVertices" in options:
                    settings.expectedVertices = int(options["expectedVertices"])
                # Vertex ID property for FK resolution
                if "typeIdProperty" in options:
                    settings.typeIdProperty = options["typeIdProperty"]
                if "typeIdType" in options:
                    settings.typeIdType = options["typeIdType"]

            elif import_type == "edges":
                settings.edges = abs_path
                if type_name:
                    settings.edgeTypeName = type_name
                if "delimiter" in options:
                    settings.edgesDelimiter = options["delimiter"]
                if "header" in options:
                    settings.edgesHeader = str(options["header"]).lower()
                if "skipEntries" in options:
                    settings.edgesSkipEntries = int(options["skipEntries"])
                if "propertiesInclude" in options:
                    settings.edgePropertiesInclude = options["propertiesInclude"]
                if "expectedEdges" in options:
                    settings.expectedEdges = int(options["expectedEdges"])

                # Edge-specific FK resolution settings
                if "edgeFromField" in options:
                    settings.edgeFromField = options["edgeFromField"]
                if "edgeToField" in options:
                    settings.edgeToField = options["edgeToField"]
                if "edgeBidirectional" in options:
                    settings.edgeBidirectional = bool(options["edgeBidirectional"])
                if "typeIdProperty" in options:
                    settings.typeIdProperty = options["typeIdProperty"]
                if "typeIdPropertyIsUnique" in options:
                    settings.typeIdPropertyIsUnique = bool(
                        options["typeIdPropertyIsUnique"]
                    )
                if "typeIdType" in options:
                    settings.typeIdType = options["typeIdType"]
                if "maxRAMIncomingEdges" in options:
                    settings.maxRAMIncomingEdges = int(options["maxRAMIncomingEdges"])

            # Common settings
            if "commitEvery" in options:
                settings.commitEvery = int(options["commitEvery"])
            if "parallel" in options:
                settings.parallel = int(options["parallel"])
            if "verboseLevel" in options:
                settings.verboseLevel = int(options["verboseLevel"])
            if "trimText" in options:
                settings.trimText = bool(options["trimText"])
            if "analysisLimitBytes" in options:
                settings.analysisLimitBytes = int(options["analysisLimitBytes"])
            if "analysisLimitEntries" in options:
                settings.analysisLimitEntries = int(options["analysisLimitEntries"])

            # Disable WAL for import (faster)
            settings.wal = False

            # Don't force database creation (we already have a database)
            settings.forceDatabaseCreate = False

            # Create Java importer with existing database
            # Pass null for url since we'll set documents/vertices/edges via setSettings
            # Java's Importer.load() checks url, documents, vertices, edges in order
            # and skips null values, so we only want to set ONE of them
            java_importer = self._JavaImporter(self._java_db, None)

            # CRITICAL: When using an existing database, openDatabase() returns early
            # without configuring async settings. We must configure them manually!
            # Note: Java method is async(), but JPype renames it to async_() to avoid
            # Python keyword conflict
            from com.arcadedb.engine import WALFile

            # Configure async settings for the import
            # Note: We do NOT start a transaction here - the Java Importer.load()
            # method manages transactions internally. If we start a transaction here,
            # the importer won't begin its own transaction (line 95-96 check) and
            # documents won't be persisted correctly.
            async_api = self._java_db.async_()
            self._java_db.setReadYourWrites(False)
            async_api.setParallelLevel(settings.parallel)
            async_api.setCommitEvery(settings.commitEvery)
            async_api.setTransactionUseWAL(settings.wal)
            async_api.setTransactionSync(WALFile.FlushType.NO)

            # Convert settings to map for setSettings method
            settings_map = {}

            # Set the appropriate source file path based on import_type
            # Java Importer.load() checks these in order: url, documents, vertices, edges
            # We only want to set ONE of these to avoid double imports!
            if import_type == "documents":
                settings_map["documents"] = settings.documents
                if settings.documentTypeName:
                    settings_map["documentType"] = settings.documentTypeName
                if settings.documentsDelimiter:
                    settings_map["documentsDelimiter"] = settings.documentsDelimiter
                if settings.documentsHeader:
                    settings_map["documentsHeader"] = str(
                        settings.documentsHeader
                    ).lower()

            elif import_type == "vertices":
                settings_map["vertices"] = settings.vertices
                if settings.vertexTypeName:
                    settings_map["vertexType"] = settings.vertexTypeName
                if settings.verticesDelimiter:
                    settings_map["verticesDelimiter"] = settings.verticesDelimiter
                if settings.verticesHeader:
                    settings_map["verticesHeader"] = str(
                        settings.verticesHeader
                    ).lower()
                if settings.typeIdProperty:
                    settings_map["typeIdProperty"] = settings.typeIdProperty
                if settings.typeIdType:
                    settings_map["typeIdType"] = settings.typeIdType

            elif import_type == "edges":
                settings_map["edges"] = settings.edges
                if settings.edgeTypeName:
                    settings_map["edgeType"] = settings.edgeTypeName
                if settings.edgesDelimiter:
                    settings_map["edgesDelimiter"] = settings.edgesDelimiter
                if settings.edgesHeader:
                    settings_map["edgesHeader"] = str(settings.edgesHeader).lower()
                if settings.edgeFromField:
                    settings_map["edgeFromField"] = settings.edgeFromField
                if settings.edgeToField:
                    settings_map["edgeToField"] = settings.edgeToField

            # Common settings
            settings_map["commitEvery"] = str(settings.commitEvery)
            settings_map["parallel"] = str(settings.parallel)
            settings_map["verboseLevel"] = str(settings.verboseLevel)
            settings_map["wal"] = "false"  # Disable WAL for faster imports

            # Apply settings via setSettings method
            java_importer.setSettings(settings_map)

            # Run import - this does all the work in Java
            # Track elapsed time since Java doesn't include it in the result map
            import time

            start_time = time.time()
            result_map = java_importer.load()

            # CRITICAL: Wait for async operations to complete
            # The async batch insert queues operations in background threads
            # We must wait for them to finish before checking results
            async_api.waitCompletion()
            elapsed_ms = int((time.time() - start_time) * 1000)

            # Convert Java result map to Python dict
            # Note: Java returns "createdDocuments", "createdVertices",
            # "createdEdges" but due to a bug in CSVImporterFormat, it doesn't
            # increment createdDocuments. So we need to query database for counts.
            # Also, Java does NOT include elapsed time in result map, so we track
            # it ourselves in Python.
            # Capture ALL keys that Java provides, don't assume we know them all
            stats = {}

            # Convert Java Map to Python dict - get ALL keys
            for key in result_map.keySet():
                value = result_map.get(key)
                stats[str(key)] = int(value) if value is not None else 0

            # Add our Python-tracked elapsed time
            stats["duration_ms"] = elapsed_ms

            # Map Java keys to Python API keys for backward compatibility
            final_stats = {
                "documents": stats.get("createdDocuments", 0),
                "vertices": stats.get("createdVertices", 0),
                "edges": stats.get("createdEdges", 0),
                "errors": stats.get("errors", 0),
                "duration_ms": elapsed_ms,
            }

            # WORKAROUND: Java CSVImporterFormat has a bug where it doesn't increment
            # the createdDocuments counter. If statistics show 0 but we know the
            # type exists, query the database to get the actual count.
            if import_type == "documents" and final_stats["documents"] == 0:
                if type_name or settings.documentTypeName:
                    target_type = type_name or settings.documentTypeName
                    try:
                        count_result = self.database.query(
                            "sql", f"SELECT count(*) as count FROM `{target_type}`"
                        )
                        count_record = list(count_result)[0]
                        actual_count = count_record.get_property("count")
                        if actual_count > 0:
                            final_stats["documents"] = actual_count
                    except:
                        # If query fails, keep the original 0
                        pass

            return final_stats

        except Exception as e:
            error_msg = str(e)

            # Check for common memory-related errors
            if any(
                mem_indicator in error_msg.lower()
                for mem_indicator in [
                    "java heap space",
                    "out of memory",
                    "outofmemoryerror",
                ]
            ):
                current_heap = os.environ.get("ARCADEDB_JVM_MAX_HEAP")
                if current_heap:
                    heap_msg = f"Current JVM heap: {current_heap}\n"
                else:
                    heap_msg = "Current JVM heap: 4g (default)\n"

                raise ArcadeDBError(
                    f"Import failed ({format_type} -> {import_type}): Out of memory.\n"
                    f"{heap_msg}"
                    f"💡 Try increasing heap size with environment variable:\n"
                    f'   export ARCADEDB_JVM_MAX_HEAP="8g"\n'
                    f"   Note: Must be set BEFORE running Python (before JVM starts)\n"
                    f"Original error: {e}"
                ) from e

            raise ArcadeDBError(
                f"Import failed ({format_type} -> {import_type}): {e}"
            ) from e

    def _import_neo4j(self, file_path: str, **options) -> Dict[str, Any]:
        """Import Neo4j JSONL export using Java Neo4jImporter."""
        from com.arcadedb.integration.importer import Neo4jImporter

        try:
            abs_path = os.path.abspath(file_path)

            # Create Neo4j importer with existing database
            java_importer = Neo4jImporter(self._java_db, abs_path)

            # Create settings map
            settings_map = {}

            # Apply options
            if "commitEvery" in options:
                settings_map["commitEvery"] = str(options["commitEvery"])
            if "verboseLevel" in options:
                settings_map["verboseLevel"] = str(options["verboseLevel"])

            # Apply settings
            java_importer.setSettings(settings_map)

            # Run import (3-pass: schema -> vertices -> edges)
            import time

            start_time = time.time()
            result_map = java_importer.load()
            elapsed_ms = int((time.time() - start_time) * 1000)

            # Convert result to Python dict - capture ALL Java stats
            stats = {}
            for key in result_map.keySet():
                value = result_map.get(key)
                stats[str(key)] = int(value) if value is not None else 0

            # Add Python-tracked elapsed time
            stats["duration_ms"] = elapsed_ms

            # Map Java keys to Python API keys for backward compatibility
            # Note: Neo4j importer may use different keys than CSV importer
            return {
                "documents": stats.get("documentCount", 0),
                "vertices": stats.get("vertexCount", 0),
                "edges": stats.get("edgeCount", 0),
                "errors": stats.get("errors", 0),
                "duration_ms": elapsed_ms,
            }

        except Exception as e:
            raise ArcadeDBError(f"Neo4j import failed: {e}") from e


# Convenience functions


def import_json(database, file_path: str, **options) -> Dict[str, Any]:
    """
    Import JSON file into database using Java importer.

    Args:
        database: Database instance
        file_path: Path to JSON file
        **options: Additional options (commitEvery, verboseLevel, etc.)

    Returns:
        Dict with import statistics

    Example:
        >>> import arcadedb_embedded as arcadedb
        >>> db = arcadedb.open_database("./mydb")
        >>> stats = arcadedb.import_json(db, "data.json")
        >>> print(f"Imported {stats['documents']} documents")
    """
    importer = Importer(database)
    return importer.import_file(
        file_path, format_type="json", import_type="documents", **options
    )


def import_csv(database, file_path: str, type_name: str, **options) -> Dict[str, Any]:
    """
    Import CSV file into database using Java importer.

    Args:
        database: Database instance
        file_path: Path to CSV file
        type_name: Target type name for imported records
        **options: Additional options:
            - import_type: 'documents', 'vertices', or 'edges' (default: 'documents')
            - delimiter: Field delimiter (e.g., ',', '\t')
            - header: Has header row (default: True)
            - commitEvery: Batch size (default: 5000)

            Edge-specific options (when import_type='edges'):
            - edgeFromField: Source vertex ID field
            - edgeToField: Target vertex ID field
            - typeIdProperty: Vertex ID property name
            - typeIdType: Type of ID ('String', 'Long', etc.)

    Returns:
        Dict with import statistics

    Examples:
        >>> # Import as documents
        >>> stats = arcadedb.import_csv(db, "people.csv", "Person")

        >>> # Import as vertices
        >>> stats = arcadedb.import_csv(
        ...     db, "users.csv", "User",
        ...     import_type="vertices"
        ... )

        >>> # Import as edges with FK resolution
        >>> stats = arcadedb.import_csv(
        ...     db, "follows.csv", "Follows",
        ...     import_type="edges",
        ...     edgeFromField="user_id",
        ...     edgeToField="follows_id",
        ...     typeIdProperty="id"
        ... )
    """
    import_type = options.pop("import_type", "documents")

    # Handle legacy options
    if "vertex_type" in options:
        import_type = "vertices"
        options.pop("vertex_type")
    if "edge_type" in options:
        import_type = "edges"
        options.pop("edge_type")

    # Legacy property name mappings
    if "from_property" in options:
        options["edgeFromField"] = options.pop("from_property")
    if "to_property" in options:
        options["edgeToField"] = options.pop("to_property")

    importer = Importer(database)
    return importer.import_file(
        file_path,
        format_type="csv",
        import_type=import_type,
        type_name=type_name,
        **options,
    )


def import_neo4j(database, file_path: str, **options) -> Dict[str, Any]:
    """
    Import Neo4j JSONL export into database using Java Neo4jImporter.

    Args:
        database: Database instance
        file_path: Path to Neo4j export file
        **options: Additional options (commitEvery, verboseLevel, etc.)

    Returns:
        Dict with import statistics

    Example:
        >>> stats = arcadedb.import_neo4j(db, "neo4j_export.jsonl")
    """
    importer = Importer(database)
    return importer.import_file(file_path, format_type="neo4j", **options)
