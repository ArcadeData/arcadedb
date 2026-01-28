"""
ArcadeDB Python Bindings - Database Export

Export functionality for ArcadeDB databases to various formats.
Supports JSONL, GraphML, GraphSON, and CSV export.
"""

import csv
import os
from typing import Any, Dict, List, Optional, Union

from .exceptions import ArcadeDBError
from .jvm import start_jvm


def export_database(
    db,
    file_path: str,
    format: str = "jsonl",
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
        format: Export format - "jsonl", "graphml", or "graphson"
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
        ArcadeDBError: If export fails or format is invalid

    Example:
        >>> # Export entire database to JSONL (recommended for backup)
        >>> stats = db.export_database("backup.jsonl.tgz", overwrite=True)
        >>> print(f"Exported {stats['totalRecords']} records in {stats['elapsedInSecs']}s")

        >>> # Export to GraphML for visualization tools (Gephi, Cytoscape)
        >>> db.export_database("graph.graphml.tgz", format="graphml", overwrite=True)

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
    if format.lower() not in supported_formats:
        raise ArcadeDBError(
            f"Invalid export format: '{format}'. "
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
        from com.arcadedb.integration.exporter import Exporter

        # Create exporter instance
        exporter = Exporter(db._java_db, file_path)

        # Configure exporter
        exporter.setFormat(format.lower())
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
            from java.util import HashMap

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
        if "Format not supported" in error_msg or "not found" in error_msg:
            raise ArcadeDBError(
                f"Export format '{format}' requires additional modules. "
                f"GraphML and GraphSON support is unavailable. Error: {error_msg}"
            ) from e
        elif "already exists" in error_msg or "cannot be overwritten" in error_msg:
            raise ArcadeDBError(
                f"Export file '{file_path}' already exists. "
                f"Use overwrite=True to replace it. Error: {error_msg}"
            ) from e
        else:
            raise ArcadeDBError(f"Database export failed: {error_msg}") from e


def export_to_csv(
    results: Union["ResultSet", List[Dict]],
    file_path: str,
    fieldnames: Optional[List[str]] = None,
):
    """
    Export query results to CSV file.

    Args:
        results: ResultSet or list of dicts to export
        file_path: Output CSV file path
        fieldnames: Column names (auto-detected if None)

    Raises:
        ArcadeDBError: If CSV export fails

    Example:
        >>> # Export query results to CSV
        >>> results = db.query("sql", "SELECT * FROM Movie LIMIT 100")
        >>> export_to_csv(results, "movies.csv")

        >>> # Or with explicit columns
        >>> export_to_csv(
        ...     results,
        ...     "movies.csv",
        ...     fieldnames=["movieId", "title", "genres"]
        ... )

        >>> # Export list of dicts
        >>> data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        >>> export_to_csv(data, "users.csv")
    """
    from .results import ResultSet

    try:
        # Convert ResultSet to list of dicts
        if isinstance(results, ResultSet):
            data = results.to_list()
        else:
            data = results

        # Ensure directory exists
        file_dir = os.path.dirname(file_path)
        if file_dir and not os.path.exists(file_dir):
            os.makedirs(file_dir, exist_ok=True)

        if not data:
            # Create empty file with headers
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                if fieldnames:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
            return

        # Auto-detect fieldnames from first record
        if fieldnames is None:
            fieldnames = list(data[0].keys())

        # Write CSV
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

    except Exception as e:
        raise ArcadeDBError(f"CSV export failed: {e}") from e
