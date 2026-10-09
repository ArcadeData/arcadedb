"""
ArcadeDB Python Bindings - Database Export

Export functionality for ArcadeDB databases to various formats.
Supports JSONL, GraphML, GraphSON, and CSV export.
"""

from __future__ import annotations

import csv
import os
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from .exceptions import ArcadeDBError
from .jvm import start_jvm

if TYPE_CHECKING:
    from .results import ResultSet


def export_database(
    db,
    file_path: str,
    export_format: str = "jsonl",
    overwrite: bool = False,
    include_types: Optional[List[str]] = None,
    exclude_types: Optional[List[str]] = None,
    verbose: int = 1,
) -> Dict[str, Any]:
    """
    Export database to file using Java Exporter.

    Args:
        db: Database instance
        file_path: Output file path (will auto-add exports/ prefix if not absolute)
        export_format: "jsonl". "graphml" and "graphson" need the engine's
            optional arcadedb-gremlin module, which this package does not
            bundle, so they raise ArcadeDBError here
        overwrite: Overwrite existing file if True
        include_types: List of types to export (None = all)
        exclude_types: List of types to exclude (None = none)
        verbose: Logging verbosity (0-2)

    Returns:
        Dictionary with export statistics:
        - totalRecords: Total records exported
        - documents: Number of documents
        - vertices: Number of vertices
        - edges: Number of edges
        - elapsedInSecs: Export duration

    Raises:
        ArcadeDBError: If export fails, the format is invalid, or the format
            needs a module this package does not bundle

    Example:
        >>> # Export entire database to JSONL (recommended for backup)
        >>> stats = db.export_database("backup.jsonl.tgz", overwrite=True)
        >>> print(
        ...     f"Exported {stats['totalRecords']} records in {stats['elapsedInSecs']}s"
        ... )

        >>> # Export specific types only
        >>> db.export_database(
        ...     "movies_only.jsonl.tgz",
        ...     include_types=["Movie", "Rating"],
        ...     overwrite=True
        ... )

    Note:
        - GraphML and GraphSON formats require GraphSON support
        - Files are saved to 'exports/' directory by default
        - JSONL format is recommended for full backup/restore
        - Exported files are compressed (.tgz format)
    """
    start_jvm()

    # Validate format
    supported_formats = ["jsonl", "graphml", "graphson"]
    if export_format.lower() not in supported_formats:
        raise ArcadeDBError(
            f"Invalid export format: '{export_format}'. "
            f"Supported formats: {', '.join(supported_formats)}"
        )

    # Ensure file_path is absolute or has exports/ prefix
    if not os.path.isabs(file_path) and not file_path.startswith("exports/"):
        file_path = os.path.join("exports", file_path)

    # Ensure exports directory exists
    export_dir = os.path.dirname(file_path) if os.path.isabs(file_path) else "exports"
    if export_dir and not os.path.exists(export_dir):
        os.makedirs(export_dir, exist_ok=True)

    try:
        import jpype

        Exporter = jpype.JClass("com.arcadedb.integration.exporter.Exporter")

        # Create exporter instance
        exporter = Exporter(db.get_java_database(), file_path)

        # Configure exporter
        exporter.setFormat(export_format.lower())
        exporter.setOverwrite(overwrite)

        # Build settings map
        settings = {}

        if include_types:
            settings["includeTypes"] = ",".join(include_types)

        if exclude_types:
            settings["excludeTypes"] = ",".join(exclude_types)

        if verbose is not None:
            settings["verboseLevel"] = str(verbose)

        if settings:
            # Convert Python dict to Java Map
            HashMap = jpype.JClass("java.util.HashMap")
            java_settings = HashMap()
            for key, value in settings.items():
                java_settings.put(key, value)
            exporter.setSettings(java_settings)

        # Execute export
        result = exporter.exportDatabase()

        # Convert Java map to Python dict
        python_result = {}
        if result:
            for key in result.keySet():
                python_result[str(key)] = result.get(key)

        return python_result

    except Exception as e:
        # Check for specific error messages
        error_msg = str(e)
        # Only GraphML and GraphSON come from arcadedb-gremlin; "not found" also matches a missing type or file.
        if export_format.lower() in ("graphml", "graphson") and (
            "arcadedb-gremlin" in error_msg
            or "Format not supported" in error_msg
            or "not found" in error_msg
        ):
            raise ArcadeDBError(
                f"Export format '{export_format}' requires additional modules: "
                f"GraphML and GraphSON come from the engine's optional "
                f"arcadedb-gremlin module, which this package does not bundle. "
                f"Use export_format='jsonl'. Error: {error_msg}"
            ) from e
        elif (
            "already exists" in error_msg
            or "already exist" in error_msg
            or "cannot be overwritten" in error_msg
        ):
            raise ArcadeDBError(
                f"Export file '{file_path}' already exists. "
                f"Use overwrite=True to replace it. Error: {error_msg}"
            ) from e
        else:
            raise ArcadeDBError(f"Database export failed: {error_msg}") from e


def _column_union(rows) -> List[str]:
    """The keys of every row, in order of first appearance. A document is
    schemaless, so the first row's keys say nothing about the others."""
    names: Dict[str, None] = {}
    for row in rows:
        for key in row:
            names.setdefault(key)
    return list(names)


def export_to_csv(
    results: Union[ResultSet, List[Dict[str, Any]]],
    file_path: str,
    fieldnames: Optional[List[str]] = None,
):
    """
    Export query results to CSV file.

    Args:
        results: ResultSet or list of dicts to export
        file_path: Output CSV file path
        fieldnames: Header and column order (auto-detected if None: the keys of
            every row, in order of first appearance; for a ResultSet, of the
            first batch of rows). It cannot rename: it must name every key of
            every row, or the export raises (for a ResultSet, after writing the
            header).

    Raises:
        ArcadeDBError: If CSV export fails

    Example:
        >>> # Export query results to CSV
        >>> results = db.query("sql", "SELECT * FROM Movie LIMIT 100")
        >>> export_to_csv(results, "movies.csv")

        >>> # Or with the columns in a chosen order
        >>> results = db.query("sql", "SELECT movieId, title, genres FROM Movie")
        >>> export_to_csv(
        ...     results,
        ...     "movies.csv",
        ...     fieldnames=["title", "movieId", "genres"]
        ... )

        >>> # Export list of dicts
        >>> data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        >>> export_to_csv(data, "users.csv")
    """
    from .results import ResultSet

    try:
        # Ensure directory exists
        file_dir = os.path.dirname(file_path)
        if file_dir and not os.path.exists(file_dir):
            os.makedirs(file_dir, exist_ok=True)

        if isinstance(results, ResultSet):
            # Stream rows via batched Java-side JSON serialization (one JPype
            # crossing per batch instead of several per row — measured ~5x on
            # 100k-row exports). Values carry JSON-native types, so DATE and
            # DATETIME columns are written as epoch-millisecond integers.
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = None
                wrote_header = False

                if fieldnames is not None:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    wrote_header = True

                detected = False
                for batch in results.iter_json_batches():
                    if not batch:
                        continue
                    if writer is None:
                        # every row of the first batch, not only its first row
                        # (#113): a row that carried a property the first row
                        # lacked raised after the header was written
                        fieldnames = _column_union(batch)
                        detected = True
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                    elif detected:
                        # The header is already written, so a column that first
                        # appears now cannot be added; say so rather than let the
                        # writer's generic ValueError stand.
                        known = set(fieldnames)
                        for key in _column_union(batch):
                            if key not in known:
                                raise ArcadeDBError(
                                    f"CSV export failed: column {key!r} first appears "
                                    "after the header was written. Pass fieldnames "
                                    "naming every column the query can return."
                                )
                    if not wrote_header:
                        writer.writeheader()
                        wrote_header = True
                    writer.writerows(batch)

                if not wrote_header and fieldnames:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()

            return

        data = results

        if not data:
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                if fieldnames:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
            return

        if fieldnames is None:
            fieldnames = _column_union(data)

        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

    except (OSError, ValueError, RuntimeError) as e:
        raise ArcadeDBError(f"CSV export failed: {e}") from e
