"""
ArcadeDB Python Bindings - Data Importer

Wrapper for ArcadeDB's Java data import functionality.
Supports importing from JSON, JSONL, CSV, and Neo4j export formats.
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from .exceptions import ArcadeDBError
from .jvm import start_jvm


class Importer:
    """
    Wrapper for ArcadeDB data importer.

    Supports importing data from:
    - JSON files (single or multiple objects)
    - JSONL files (line-delimited JSON)
    - CSV/TSV files
    - Neo4j JSONL exports

    The importer uses streaming parsers for memory efficiency and
    performs batch transactions for optimal performance.
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

    def import_file(
        self,
        file_path: str,
        format_type: Optional[str] = None,
        type_name: Optional[str] = None,
        commit_every: int = 1000,
        **options,
    ) -> Dict[str, Any]:
        """
        Import data from a file. Auto-detects format if not specified.

        Args:
            file_path: Path to the file to import
            format_type: Format type: 'json', 'jsonl', 'csv', 'neo4j'
                        (auto-detected from extension if None)
            type_name: Target document/vertex type name for CSV imports
            commit_every: Commit transaction every N records (default: 1000)
            **options: Additional format-specific options:

                CSV options:
                - delimiter: Field delimiter (default: ',')
                - quote_char: Quote character (default: '"')
                - header: Has header row (default: True)
                - vertex_type: For vertex imports
                - edge_type: For edge imports
                - from_property: Property for edge source
                - to_property: Property for edge target

                JSON options:
                - mapping: Dict mapping JSON paths to database types

                JSONL options:
                - parse_rids: Parse RID references (default: True)

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
            >>> importer = Importer(db)
            >>> stats = importer.import_file('data.csv', type_name='Person')
            >>> print(f"Imported {stats['documents']} documents")
        """
        if not os.path.exists(file_path):
            raise ArcadeDBError(f"File not found: {file_path}")

        # Auto-detect format from extension if not specified
        if format_type is None:
            ext = Path(file_path).suffix.lower()
            format_map = {
                ".json": "json",
                ".jsonl": "jsonl",
                ".csv": "csv",
                ".tsv": "csv",
                ".txt": "jsonl",
            }
            format_type = format_map.get(ext)
            if format_type is None:
                raise ArcadeDBError(
                    f"Cannot auto-detect format for extension: {ext}. "
                    "Please specify format_type explicitly."
                )

        # Route to appropriate importer
        format_type = format_type.lower()
        if format_type == "json":
            return self._import_json(file_path, commit_every, **options)
        elif format_type == "jsonl":
            return self._import_jsonl(file_path, commit_every, type_name, **options)
        elif format_type == "csv":
            if type_name is None:
                raise ArcadeDBError("type_name is required for CSV imports")
            return self._import_csv(file_path, type_name, commit_every, **options)
        elif format_type == "neo4j":
            return self._import_neo4j(file_path, commit_every, **options)
        else:
            raise ArcadeDBError(
                f"Unsupported format: {format_type}. "
                "Supported formats: json, jsonl, csv, neo4j"
            )

    def _import_json(
        self, file_path: str, commit_every: int, **options
    ) -> Dict[str, Any]:
        """Import JSON file using Java JSONImporterFormat."""
        from com.arcadedb.integration.importer import Importer as JavaImporter
        from com.arcadedb.integration.importer import ImporterSettings

        try:
            settings = ImporterSettings()
            settings.commitEvery = commit_every
            # Note: verbose field may not exist on all Java API versions

            # Convert Python options to Java settings
            if "mapping" in options:
                # TODO: Implement mapping rules conversion
                pass

            java_importer = JavaImporter(
                ["-i", file_path, "-d", self._java_db.getDatabasePath()], settings
            )

            # Run import
            java_importer.run()

            # Get statistics
            context = java_importer.getContext()
            return {
                "documents": context.documentCount,
                "vertices": context.vertexCount,
                "edges": context.edgeCount,
                "errors": context.errors,
                "duration_ms": context.elapsed,
            }

        except Exception as e:
            raise ArcadeDBError(f"JSON import failed: {e}") from e

    def _import_jsonl(
        self,
        file_path: str,
        commit_every: int,
        type_name: Optional[str] = None,
        **options,
    ) -> Dict[str, Any]:
        """
        Import JSONL (line-delimited JSON) using Python.

        Each line is a JSON object. Imports as documents by default,
        or as vertices if vertex_type is specified in options.
        """
        import json

        # type_name can be passed directly or in options
        if type_name is None:
            type_name = options.get("type_name")
        if not type_name:
            raise ArcadeDBError("type_name is required for JSONL import")

        doc_count = 0
        vertex_count = 0
        error_count = 0
        is_vertex = options.get("vertex_type") is not None

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        # Parse JSON line
                        data = json.loads(line)

                        # Start transaction if needed
                        count = doc_count + vertex_count
                        if count % commit_every == 0:
                            if count > 0:
                                self.database.commit()
                            self.database.begin()

                        # Create document or vertex
                        if is_vertex:
                            record = self._java_db.newVertex(type_name)
                            vertex_count += 1
                        else:
                            record = self._java_db.newDocument(type_name)
                            doc_count += 1

                        for key, value in data.items():
                            record.set(key, value)
                        record.save()

                    except json.JSONDecodeError:
                        error_count += 1
                    except Exception:
                        error_count += 1

                # Commit final batch
                if (doc_count + vertex_count) % commit_every != 0:
                    self.database.commit()

            return {
                "documents": doc_count,
                "vertices": vertex_count,
                "edges": 0,
                "errors": error_count,
                "duration_ms": 0,
            }

        except Exception as e:
            raise ArcadeDBError(f"JSONL import failed: {e}") from e

    def _import_csv(
        self, file_path: str, type_name: str, commit_every: int, **options
    ) -> Dict[str, Any]:
        """
        Import CSV file.

        Supports importing as documents, vertices, or edges.
        """
        import time

        try:
            delimiter = options.pop("delimiter", ",")
            has_header = options.pop("header", True)
            vertex_type = options.pop("vertex_type", None)
            edge_type = options.pop("edge_type", None)

            start_time = time.time()

            if edge_type:
                # Import as edges
                stats = self._import_csv_as_edges(
                    file_path, edge_type, commit_every, delimiter, has_header, **options
                )
            elif vertex_type:
                # Import as vertices
                stats = self._import_csv_as_vertices(
                    file_path,
                    vertex_type,
                    commit_every,
                    delimiter,
                    has_header,
                    **options,
                )
            else:
                # Import as documents (default)
                stats = self._import_csv_as_documents(
                    file_path, type_name, commit_every, delimiter, has_header, **options
                )

            stats["duration_ms"] = int((time.time() - start_time) * 1000)
            return stats

        except Exception as e:
            raise ArcadeDBError(f"CSV import failed: {e}") from e

    def _import_csv_as_documents(
        self,
        file_path: str,
        type_name: str,
        commit_every: int,
        delimiter: str,
        has_header: bool,
        **options,
    ) -> Dict[str, Any]:
        """Import CSV as documents using Python CSV reader."""
        import csv

        doc_count = 0
        error_count = 0

        with open(file_path, "r", encoding="utf-8") as f:
            if delimiter == "\\t":
                delimiter = "\t"

            reader = csv.reader(f, delimiter=delimiter)

            # Read header
            if has_header:
                headers = next(reader)
            else:
                # Generate column names
                first_row = next(reader)
                headers = [f"col_{i}" for i in range(len(first_row))]
                # Process first row
                self.database.begin()
                doc = self.database.new_document(type_name)
                for header, value in zip(headers, first_row):
                    doc.set(header, value)
                doc.save()
                doc_count += 1

            # Import data
            self.database.begin()
            batch_count = 0

            for row in reader:
                try:
                    doc = self.database.new_document(type_name)
                    for header, value in zip(headers, row):
                        # Try to infer type
                        typed_value = self._infer_type(value)
                        doc.set(header, typed_value)
                    doc.save()

                    doc_count += 1
                    batch_count += 1

                    if batch_count >= commit_every:
                        self.database.commit()
                        self.database.begin()
                        batch_count = 0

                except Exception as e:
                    error_count += 1
                    if options.get("verbose", False):
                        print(f"Error importing row: {e}")

            # Commit remaining
            if batch_count > 0:
                self.database.commit()

        return {
            "documents": doc_count,
            "vertices": 0,
            "edges": 0,
            "errors": error_count,
        }

    def _import_csv_as_vertices(
        self,
        file_path: str,
        vertex_type: str,
        commit_every: int,
        delimiter: str,
        has_header: bool,
        **options,
    ) -> Dict[str, Any]:
        """Import CSV as vertices."""
        import csv

        vertex_count = 0
        error_count = 0

        with open(file_path, "r", encoding="utf-8") as f:
            if delimiter == "\\t":
                delimiter = "\t"

            reader = csv.reader(f, delimiter=delimiter)

            # Read header
            if has_header:
                headers = next(reader)
            else:
                first_row = next(reader)
                headers = [f"col_{i}" for i in range(len(first_row))]
                # Process first row
                self.database.begin()
                vertex = self.database.new_vertex(vertex_type)
                for header, value in zip(headers, first_row):
                    vertex.set(header, self._infer_type(value))
                vertex.save()
                vertex_count += 1

            # Import data
            self.database.begin()
            batch_count = 0

            for row in reader:
                try:
                    vertex = self.database.new_vertex(vertex_type)
                    for header, value in zip(headers, row):
                        typed_value = self._infer_type(value)
                        vertex.set(header, typed_value)
                    vertex.save()

                    vertex_count += 1
                    batch_count += 1

                    if batch_count >= commit_every:
                        self.database.commit()
                        self.database.begin()
                        batch_count = 0

                except Exception as e:
                    error_count += 1
                    if options.get("verbose", False):
                        print(f"Error importing row: {e}")

            # Commit remaining
            if batch_count > 0:
                self.database.commit()

        return {
            "documents": 0,
            "vertices": vertex_count,
            "edges": 0,
            "errors": error_count,
        }

    def _import_csv_as_edges(
        self,
        file_path: str,
        edge_type: str,
        commit_every: int,
        delimiter: str,
        has_header: bool,
        **options,
    ) -> Dict[str, Any]:
        """Import CSV as edges (requires from/to columns)."""
        import csv

        from_property = options.get("from_property", "from")
        to_property = options.get("to_property", "to")

        edge_count = 0
        error_count = 0

        with open(file_path, "r", encoding="utf-8") as f:
            if delimiter == "\\t":
                delimiter = "\t"

            reader = csv.DictReader(f, delimiter=delimiter) if has_header else None

            if not has_header:
                raise ArcadeDBError(
                    "Edge imports require a header row with column names"
                )

            # Verify required columns
            if from_property not in reader.fieldnames:
                raise ArcadeDBError(f"Missing required column: {from_property}")
            if to_property not in reader.fieldnames:
                raise ArcadeDBError(f"Missing required column: {to_property}")

            # Import edges
            self.database.begin()
            batch_count = 0

            for row in reader:
                try:
                    from_rid = row[from_property]
                    to_rid = row[to_property]

                    # Create edge using SQL
                    props = []
                    for key, value in row.items():
                        if key not in [from_property, to_property]:
                            typed_value = self._infer_type(value)
                            if isinstance(typed_value, str):
                                props.append(f"{key} = '{typed_value}'")
                            else:
                                props.append(f"{key} = {typed_value}")

                    props_str = ", ".join(props) if props else ""
                    if props_str:
                        props_str = f"SET {props_str}"

                    sql = f"""
                        CREATE EDGE {edge_type}
                        FROM {from_rid}
                        TO {to_rid}
                        {props_str}
                    """

                    self.database.command("sql", sql)

                    edge_count += 1
                    batch_count += 1

                    if batch_count >= commit_every:
                        self.database.commit()
                        self.database.begin()
                        batch_count = 0

                except Exception as e:
                    error_count += 1
                    if options.get("verbose", False):
                        print(f"Error importing edge: {e}")

            # Commit remaining
            if batch_count > 0:
                self.database.commit()

        return {
            "documents": 0,
            "vertices": 0,
            "edges": edge_count,
            "errors": error_count,
        }

    def _import_neo4j(
        self, file_path: str, commit_every: int, **options
    ) -> Dict[str, Any]:
        """Import Neo4j JSONL export using Java Neo4jImporter."""
        from com.arcadedb.integration.importer import ImporterSettings, Neo4jImporter

        try:
            settings = ImporterSettings()
            settings.commitEvery = commit_every
            settings.verbose = options.get("verbose", False)

            java_importer = Neo4jImporter(
                ["-i", file_path, "-d", self._java_db.getDatabasePath()], settings
            )

            # Run import (3-pass: schema -> vertices -> edges)
            java_importer.run()

            # Get statistics
            context = java_importer.getContext()
            return {
                "documents": context.documentCount,
                "vertices": context.vertexCount,
                "edges": context.edgeCount,
                "errors": context.errors,
                "duration_ms": context.elapsed,
            }

        except Exception as e:
            raise ArcadeDBError(f"Neo4j import failed: {e}") from e

    def _infer_type(self, value: str) -> Any:
        """Try to infer the type of a string value."""
        if not value or value == "":
            return None

        # Try boolean
        if value.lower() in ("true", "false"):
            return value.lower() == "true"

        # Try integer
        try:
            return int(value)
        except ValueError:
            pass

        # Try float
        try:
            return float(value)
        except ValueError:
            pass

        # Return as string
        return value


# Convenience functions


def import_json(database, file_path: str, **options) -> Dict[str, Any]:
    """
    Import JSON file into database.

    Args:
        database: Database instance
        file_path: Path to JSON file
        **options: Additional options (commit_every, mapping, etc.)

    Returns:
        Dict with import statistics

    Example:
        >>> import arcadedb_embedded as arcadedb
        >>> db = arcadedb.open_database("./mydb")
        >>> stats = arcadedb.import_json(db, "data.json")
        >>> print(f"Imported {stats['documents']} documents")
    """
    importer = Importer(database)
    return importer.import_file(file_path, format_type="json", **options)


def import_jsonl(database, file_path: str, type_name: str, **options) -> Dict[str, Any]:
    """
    Import JSONL (line-delimited JSON) file into database.

    Args:
        database: Database instance
        file_path: Path to JSONL file
        type_name: Target type name for imported records
        **options: Additional options (commit_every, etc.)

    Returns:
        Dict with import statistics

    Example:
        >>> stats = arcadedb.import_jsonl(db, "data.jsonl", "User")
    """
    importer = Importer(database)
    return importer.import_file(
        file_path, format_type="jsonl", type_name=type_name, **options
    )


def import_csv(database, file_path: str, type_name: str, **options) -> Dict[str, Any]:
    """
    Import CSV file into database.

    Args:
        database: Database instance
        file_path: Path to CSV file
        type_name: Target type name for imported records
        **options: Additional options:
            - delimiter: Field delimiter (default: ',')
            - header: Has header row (default: True)
            - vertex_type: Import as vertices (optional)
            - edge_type: Import as edges (optional, requires from/to columns)
            - commit_every: Batch size (default: 1000)

    Returns:
        Dict with import statistics

    Examples:
        >>> # Import as documents
        >>> stats = arcadedb.import_csv(db, "people.csv", "Person")

        >>> # Import as vertices
        >>> stats = arcadedb.import_csv(
        ...     db, "users.csv", "User",
        ...     vertex_type="User"
        ... )

        >>> # Import as edges
        >>> stats = arcadedb.import_csv(
        ...     db, "follows.csv", "Follows",
        ...     edge_type="Follows",
        ...     from_property="user_id",
        ...     to_property="follows_id"
        ... )
    """
    importer = Importer(database)
    return importer.import_file(
        file_path, format_type="csv", type_name=type_name, **options
    )


def import_neo4j(database, file_path: str, **options) -> Dict[str, Any]:
    """
    Import Neo4j JSONL export into database.

    Args:
        database: Database instance
        file_path: Path to Neo4j export file
        **options: Additional options (commit_every, etc.)

    Returns:
        Dict with import statistics

    Example:
        >>> stats = arcadedb.import_neo4j(db, "neo4j_export.jsonl")
    """
    importer = Importer(database)
    return importer.import_file(file_path, format_type="neo4j", **options)
