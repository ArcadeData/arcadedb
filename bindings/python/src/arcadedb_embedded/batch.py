"""
Batch processing context manager for ArcadeDB.

Provides a high-level interface for bulk operations using the async executor
with automatic configuration, progress tracking, and error handling.
"""

import sys
from typing import Any, Callable, Dict, List, Optional

from .graph import Vertex
from .type_conversion import convert_python_to_java
from .vector import to_java_float_array


class BatchContext:
    """
    High-level batch processing context manager.

    Provides simplified API for bulk operations with:
    - Automatic async executor setup and cleanup
    - Configurable batch size and parallelism
    - Optional progress tracking
    - Error collection and reporting
    - Convenience methods for common operations

    Example:
        >>> with db.batch_context(batch_size=5000, parallel=8) as batch:
        ...     for record in large_dataset:
        ...         batch.create_vertex("User", name=record['name'])
        >>> # Auto-commits and waits for completion on exit

    Example with progress:
        >>> with db.batch_context(batch_size=5000, progress=True) as batch:
        ...     batch.set_total(len(dataset))
        ...     for record in dataset:
        ...         batch.create_vertex("User", **record)
        >>> # Prints progress bar automatically
    """

    def __init__(
        self,
        db,
        batch_size: int = 5000,
        parallel: int = 4,
        use_wal: bool = True,
        back_pressure: int = 50,
        progress: bool = False,
        progress_desc: str = "Processing",
    ):
        """
        Initialize batch context.

        Args:
            db: Database instance
            batch_size: Auto-commit every N operations (default: 5000)
            parallel: Number of parallel worker threads 1-16 (default: 4)
            use_wal: Enable Write-Ahead Log (default: True)
            back_pressure: Queue back-pressure threshold 0-100 (default: 50)
            progress: Enable progress tracking (default: False)
            progress_desc: Description for progress bar (default: "Processing")
        """
        self.db = db
        self.batch_size = batch_size
        self.parallel = parallel
        self.use_wal = use_wal
        self.back_pressure = back_pressure
        self.progress_enabled = progress
        self.progress_desc = progress_desc

        self.async_exec = None
        self.errors: List[Exception] = []
        self.success_count = 0
        self.total = None
        self.progress_bar = None

    def __enter__(self):
        """Enter context manager - setup async executor."""
        # Get async executor from database
        self.async_exec = self.db.async_executor()

        # Configure async executor
        self.async_exec.set_commit_every(self.batch_size)
        self.async_exec.set_parallel_level(self.parallel)
        self.async_exec.set_transaction_use_wal(self.use_wal)
        if self.back_pressure > 0:
            self.async_exec.set_back_pressure(self.back_pressure)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager - wait for completion and cleanup."""
        if self.async_exec:
            # Wait for all operations to complete
            self.async_exec.wait_completion()

            # Close the async executor to shutdown worker threads
            self.async_exec.close()

            # Close progress bar if enabled
            if self.progress_bar is not None:
                try:
                    self.progress_bar.close()
                except Exception:
                    pass

        # Don't suppress exceptions
        return False

    def set_total(self, total: int):
        """
        Set total number of operations for progress tracking.

        Args:
            total: Total number of operations expected
        """
        self.total = total
        if self.progress_enabled:
            self._init_progress_bar()

    def _init_progress_bar(self):
        """Initialize progress bar if tqdm is available."""
        if self.total is None:
            return

        try:
            from tqdm import tqdm

            self.progress_bar = tqdm(
                total=self.total, desc=self.progress_desc, unit="records"
            )
        except ImportError:
            # tqdm not available, print simple progress
            print(f"{self.progress_desc}: 0/{self.total}", file=sys.stderr)

    def _update_progress(self):
        """Update progress bar or print progress."""
        self.success_count += 1

        if self.progress_bar is not None:
            self.progress_bar.update(1)
        elif self.progress_enabled and self.total is not None:
            # Simple progress without tqdm
            if self.success_count % 1000 == 0:
                pct = (self.success_count / self.total) * 100
                print(
                    f"\r{self.progress_desc}: {self.success_count}/{self.total} "
                    f"({pct:.1f}%)",
                    file=sys.stderr,
                    end="",
                )

    def _make_callback(self, user_callback: Optional[Callable] = None):
        """
        Create callback that updates progress and calls user callback.

        Args:
            user_callback: Optional user callback function

        Returns:
            Callback function for async operations
        """

        def callback(record):
            self._update_progress()
            if user_callback:
                user_callback(record)

        return callback

    def _make_edge_callback(self, user_callback: Optional[Callable] = None):
        """
        Create edge callback that updates progress and calls user callback.

        Edge callbacks receive 3 parameters: edge, created_source, created_dest.

        Args:
            user_callback: Optional user callback function

        Returns:
            Callback function for edge operations
        """

        def callback(edge, created_source, created_dest):
            self._update_progress()
            if user_callback:
                user_callback(edge, created_source, created_dest)

        return callback

    def _make_error_callback(self, user_error_callback: Optional[Callable] = None):
        """
        Create error callback that collects errors and calls user callback.

        Args:
            user_error_callback: Optional user error callback function

        Returns:
            Error callback function for async operations
        """

        def error_callback(error):
            self.errors.append(error)
            if user_error_callback:
                user_error_callback(error)

        return error_callback

    def create_vertex(
        self, type_name: str, callback: Optional[Callable] = None, **properties
    ):
        """
        Create a vertex asynchronously.

        Args:
            type_name: Vertex type name
            callback: Optional callback for success
            **properties: Vertex properties as keyword arguments

        Example:
            >>> batch.create_vertex("User", name="Alice", age=30)
        """
        if self.async_exec is None:
            raise RuntimeError("BatchContext not entered")

        vertex = self.db.new_vertex(type_name)
        for key, value in properties.items():
            # Auto-convert numpy arrays to Java float arrays
            if hasattr(value, "dtype") and hasattr(value, "tolist"):
                try:
                    import numpy as np

                    if np.issubdtype(value.dtype, np.floating):
                        value = to_java_float_array(value)
                except ImportError:
                    pass

            vertex.set(key, value)

        cb = self._make_callback(callback)
        self.async_exec.create_record(vertex, callback=cb)

    def create_document(
        self, type_name: str, callback: Optional[Callable] = None, **properties
    ):
        """
        Create a document asynchronously.

        Args:
            type_name: Document type name
            callback: Optional callback for success
            **properties: Document properties as keyword arguments

        Example:
            >>> batch.create_document("LogEntry", message="Test", level="INFO")
        """
        if self.async_exec is None:
            raise RuntimeError("BatchContext not entered")

        document = self.db.new_document(type_name)
        for key, value in properties.items():
            # Auto-convert numpy arrays to Java float arrays
            if hasattr(value, "dtype") and hasattr(value, "tolist"):
                try:
                    import numpy as np

                    if np.issubdtype(value.dtype, np.floating):
                        value = to_java_float_array(value)
                except ImportError:
                    pass

            document.set(key, value)

        cb = self._make_callback(callback)
        self.async_exec.create_record(document, callback=cb)

    def create_edge(
        self,
        from_vertex,
        to_vertex,
        edge_type: str,
        callback: Optional[Callable] = None,
        **properties,
    ):
        """
        Create an edge synchronously within the batch context.

        Note: Edge creation with new_edge() is synchronous and immediately
        persists the edge, so it cannot be queued asynchronously like
        vertex/document creation. The edge is created immediately but
        within the batch context's transaction.

        Bidirectionality is determined by the EdgeType schema definition,
        not by a per-call parameter.

        Args:
            from_vertex: Source vertex (Python Vertex or Java vertex object)
            to_vertex: Destination vertex (Python Vertex or Java vertex object)
            edge_type: Edge type name
            callback: Optional callback for success (called immediately after creation)
            **properties: Edge properties as keyword arguments

        Example:
            >>> batch.create_edge(user, movie, "RATED", rating=5)
        """
        if self.async_exec is None:
            raise RuntimeError("BatchContext not entered")

        java_from = (
            from_vertex._java_document
            if isinstance(from_vertex, Vertex)
            else from_vertex
        )
        java_to = (
            to_vertex._java_document if isinstance(to_vertex, Vertex) else to_vertex
        )

        # Convert properties to Java format
        props = {}
        for key, value in properties.items():
            props[key] = convert_python_to_java(value)

        # Edge creation is synchronous - new_edge() persists immediately
        # Use Pythonic API (bidirectionality determined by EdgeType schema)
        if not isinstance(java_from, Vertex):
            from_vertex_wrapped = Vertex(java_from)
        else:
            from_vertex_wrapped = java_from

        edge = from_vertex_wrapped.new_edge(edge_type, java_to, **props)

        # Update progress tracking
        self._update_progress()

        # Call user callback if provided
        if callback:
            callback(edge)

    def create_record(self, record, callback: Optional[Callable] = None):
        """
        Create any record (vertex, document, or edge) asynchronously.

        Args:
            record: Java record object (MutableVertex, MutableDocument, MutableEdge)
            callback: Optional callback for success

        Example:
            >>> vertex = db.new_vertex("User")
            >>> vertex.set("name", "Bob")
            >>> batch.create_record(vertex)
        """
        if self.async_exec is None:
            raise RuntimeError("BatchContext not entered")

        cb = self._make_callback(callback)
        self.async_exec.create_record(record, callback=cb)

    def update_record(self, record, callback: Optional[Callable] = None):
        """
        Update a record asynchronously.

        Args:
            record: Java record object to update
            callback: Optional callback for success
        """
        if self.async_exec is None:
            raise RuntimeError("BatchContext not entered")

        cb = self._make_callback(callback)
        self.async_exec.update_record(record, callback=cb)

    def delete_record(self, record, callback: Optional[Callable] = None):
        """
        Delete a record asynchronously.

        Args:
            record: Java record object to delete
            callback: Optional callback for success
        """
        if self.async_exec is None:
            raise RuntimeError("BatchContext not entered")

        cb = self._make_callback(callback)
        self.async_exec.delete_record(record, callback=cb)

    def query(self, language: str, query: str, callback: Callable, **params):
        """
        Execute async query.

        Args:
            language: Query language (e.g., "sql")
            query: Query string
            callback: Callback for query results
            **params: Query parameters
        """
        if self.async_exec is None:
            raise RuntimeError("BatchContext not entered")

        self.async_exec.query(language, query, callback, **params)

    def command(self, language: str, command: str, callback: Callable, **params):
        """
        Execute async command.

        Args:
            language: Command language (e.g., "sql")
            command: Command string
            callback: Callback for command results
            **params: Command parameters
        """
        if self.async_exec is None:
            raise RuntimeError("BatchContext not entered")

        self.async_exec.command(language, command, callback, **params)

    def is_pending(self) -> bool:
        """
        Check if any operations are still pending.

        Returns:
            True if operations are pending, False otherwise
        """
        if self.async_exec is None:
            return False
        return self.async_exec.is_pending()

    def wait_completion(self, timeout: Optional[float] = None):
        """
        Wait for all pending operations to complete.

        Args:
            timeout: Optional timeout in seconds
        """
        if self.async_exec is not None:
            self.async_exec.wait_completion(timeout)

    def get_errors(self) -> List[Exception]:
        """
        Get list of errors collected during batch processing.

        Returns:
            List of exceptions that occurred
        """
        return self.errors

    def get_success_count(self) -> int:
        """
        Get count of successful operations.

        Returns:
            Number of successful operations
        """
        return self.success_count
